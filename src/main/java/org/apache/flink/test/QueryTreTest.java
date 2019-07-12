package org.apache.flink.test;

import org.apache.flink.entities.Comment;
import org.apache.flink.entities.CommentSchema;
import org.apache.flink.utils.JedisPoolFactory;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.util.*;

public class QueryTreTest {

    public static void main(String[] args) throws Exception {

        final int daily_Window_size = 24;
        final int weekly_Window_size = 24 * 7;
        final int monthly_Window_size = 24 * 30;

        final int window_dimension = daily_Window_size;

        //String file_path = "query3_output_" + window_dimension + ".txt";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "flink");

        JedisPoolFactory.init("localhost", 6379);

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").build();

        DataStream<Comment> stream = env.addSource(new FlinkKafkaConsumer<>("flink", new CommentSchema(), properties))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Comment>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(Comment comment) {
                        return comment.createDate * 1000;
                    }
                }).filter(x -> x.userID !=0);


        DataStream<Comment> originalComments = stream.filter(x -> x.depth == 1);


        /**
         * SALVATAGGIO COMMENTI DEPTH 1 SU REDIS
         * output: tuple(id comment, id autore)
         * redis sink, string set
         * salvataggio su redis delle informazioni (id commento / id autore) dei commenti depth 1
         */
        DataStreamSink<Tuple3<String, String,Long>> commentAuthor = originalComments.map(new MapFunction<Comment, Tuple3<String, String, Long>>() {
            @Override
            public Tuple3<String, String, Long> map(Comment comment) throws Exception {
                return new Tuple3<>(Integer.toString(comment.commentID), Integer.toString(comment.userID), comment.arrivalTime);
            }
        }).addSink(new RedisSink<>(conf, new RedisDepthOne()));

        /**
         * depth1
         * output: (autore, likes)
         * estraggo i like per ciascun commento
         */
        DataStream<Tuple3<String, Double,Long>> directCommentsLikes = originalComments
                .map(new MapFunction<Comment, Tuple3<String, Double,Long>>() {
                    @Override
                    public Tuple3<String, Double,Long> map(Comment comment) {

                        String author = Integer.toString(comment.userID);
                        Double likes = new Double(comment.recommendations);
                        if(comment.editorsSelection){
                            int increment = (int) (likes*0.1);
                            likes += increment;
                        }

                        return new Tuple3<>(author,likes*0.3, comment.arrivalTime);
                    }
                }).keyBy(0).timeWindow(Time.hours(window_dimension)).reduce(new ReduceFunction<Tuple3<String, Double, Long>>() {
                    @Override
                    public Tuple3<String, Double, Long> reduce(Tuple3<String, Double, Long> t1, Tuple3<String, Double, Long> t2) throws Exception {

                        long v = 0;
                        if(t1.f2 > t2.f2){
                            v = t1.f2;
                        }else{
                            v = t2.f2;
                        }

                        return new Tuple3<>(t1.f0, t1.f1 + t2.f1,v);
                    }
                });


        /**
         * DEPTH 2
         * output: tuple(id commento padre, id user padre, id user nonno
         * tuple pronte per essere salvate su redis
         */
        DataStream<Tuple4<String,String,String,Long>> twoStream = stream.filter(x -> x.depth == 2)
                .flatMap(new MapFunctionDepthTest2());

        /**
         * salvataggio su redis delle tuple depth 2
         */
        DataStreamSink<Tuple4<String,String,String,Long>> sinkTwo = twoStream.addSink(new RedisSink<>(conf, new RedisDepthTwo()));

        DataStream<Tuple3<String,Double,Long>> commCountDepthTwo = twoStream.map(new MapFunction<Tuple4<String, String, String, Long>, Tuple3<String, Double, Long>>() {
            @Override
            public Tuple3<String, Double, Long> map(Tuple4<String, String, String, Long> stringStringStringLongTuple4) throws Exception {
                return new Tuple3<>(stringStringStringLongTuple4.f2, 1*0.7, stringStringStringLongTuple4.f3);
            }
        }).keyBy(0).timeWindow(Time.hours(window_dimension)).reduce(new ReduceFunction<Tuple3<String, Double, Long>>() {
            @Override
            public Tuple3<String, Double, Long> reduce(Tuple3<String, Double, Long> t1, Tuple3<String, Double, Long> t2) throws Exception {

                long v = 0;
                if(t1.f2 > t2.f2){
                    v = t1.f2;
                }else{
                    v = t2.f2;
                }

                return new Tuple3<>(t1.f0, t1.f1+t1.f2,v);
            }
        });

        /**
         * DEPTH 3
         * output: tuple(autore,count comm)
         * calcola le tuple autore/count per i commenti di depth 3
         */
        DataStream<Tuple3<String, Double, Long>> commCountDepthThree = stream.filter(x -> x.depth == 3)
                .flatMap(new MapFunctionDepthTest3())
                .keyBy(0)
                .timeWindow(Time.hours(window_dimension))
                .reduce(new ReduceFunction<Tuple3<String, Double, Long>>() {
                    @Override
                    public Tuple3<String, Double, Long> reduce(Tuple3<String, Double, Long> t1, Tuple3<String, Double, Long> t2) throws Exception {

                        long v = 0;
                        if(t1.f2 > t2.f2){
                            v = t1.f2;
                        }else{
                            v = t2.f2;
                        }

                        return new Tuple3<>(t1.f0,t1.f1+t2.f1,v);
                    }
                });

        /**
         * output: tuple(autore/count comm totale * peso)
         * valori finali per i commenti
         */
        DataStream<Tuple3<String, Double,Long>> joinDepthTwoThree = commCountDepthTwo.union(commCountDepthThree)
                .keyBy(0)
                .reduce(new ReduceFunction<Tuple3<String, Double, Long>>() {
                    @Override
                    public Tuple3<String, Double, Long> reduce(Tuple3<String, Double, Long> t1, Tuple3<String, Double, Long> t2) throws Exception {
                        long v = 0;
                        if(t1.f2 > t2.f2){
                            v = t1.f2;
                        }else{
                            v = t2.f2;
                        }

                        return new Tuple3<>(t1.f0,t1.f1+t2.f1,v);
                    }
                });

        /**
         * output: tuple(autore/rank)
         * somma dei rank per commenti e like per ciascun user
         */
        DataStream<Tuple3<String, Double,Long>> windowResults = directCommentsLikes.union(joinDepthTwoThree)
                .keyBy(0).reduce(new ReduceFunction<Tuple3<String, Double, Long>>() {
                    @Override
                    public Tuple3<String, Double, Long> reduce(Tuple3<String, Double, Long> t1, Tuple3<String, Double, Long> t2) throws Exception {
                        long v = 0;
                        if(t1.f2 > t2.f2){
                            v = t1.f2;
                        }else{
                            v = t2.f2;
                        }

                        return new Tuple3<>(t1.f0,t1.f1+t2.f1,v);
                    }
                });

        /**
         * ranking
         * top 10
         */
        DataStream<String> resultRanking = windowResults.timeWindowAll(Time.hours(window_dimension)).apply(new AllWindowFunction<Tuple3<String, Double, Long>, String, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<Tuple3<String, Double, Long>> iterable, Collector<String> collector) throws Exception {

                List<Tuple3<String,Double,Long>> myList = new ArrayList<>();
                for(Tuple3<String,Double, Long> t : iterable){
                    myList.add(t);
                }

                Collections.sort(myList, new Comparator<Tuple3<String, Double,Long>>() {
                    @Override
                    public int compare(Tuple3<String, Double,Long> o1, Tuple3<String, Double,Long> o2) {
                        int first = (int) (o1.f1 * 1000);
                        int second = (int) (o2.f1 * 1000);
                        return first - second;
                    }
                });

                String result = "" + new Date(timeWindow.getStart()) +"";

                for (int i = 0; i < 10 && i < myList.size(); i++) {
                    result += ", "+ myList.get(i).f0 +", "+myList.get(i).f1;
                }

                //System.out.println(result);

                collector.collect(result);

                long current = System.currentTimeMillis();
                long start = 0;

                for (int i = 0; i < myList.size() ; i++) {
                    if(myList.get(0).f2 > start){
                        start = myList.get(0).f2;
                    }
                }

                System.out.println((current-start));

            }
        });

        env.execute();

    }

    public static class RedisDepthOne implements RedisMapper<Tuple3<String, String, Long>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SET, "SET_NAME");
        }

        @Override
        public String getKeyFromData(Tuple3<String, String, Long> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple3<String, String, Long> data) {
            return data.f1;
        }
    }

    public static class RedisDepthTwo implements RedisMapper<Tuple4<String,String,String,Long>>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SET, "SET_NAME");
        }

        @Override
        public String getKeyFromData(Tuple4<String,String,String,Long> stringStringStringTuple3) {
            return stringStringStringTuple3.f0;
        }

        @Override
        public String getValueFromData(Tuple4<String,String,String,Long> stringStringStringTuple3) {
            String v = stringStringStringTuple3.f1 +"_"+stringStringStringTuple3.f2;
            return v;
        }
    }
}
