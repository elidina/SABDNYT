package org.apache.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.entities.Comment;
import org.apache.flink.entities.CommentSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.utils.JedisPoolFactory;
import org.apache.flink.utils.MapFunctionDepth2;
import org.apache.flink.utils.MapFunctionDepth3;

import java.util.*;

public class QueryTre {

    public static void main(String[] args) throws Exception {

        final int daily_Window_size = 24;
        final int weekly_Window_size = 24*7;
        final int monthly_Window_size = 24*30;

        final int window_dimension = daily_Window_size;

        String file_path = "query3_output_"+window_dimension+".txt";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        //properties.setProperty("bootstrap.servers", "broker:29092");
        //properties.setProperty("zookeeper.connect", "broker:2181");
        properties.setProperty("group.id", "flink");

        JedisPoolFactory.init("localhost", 6379);

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").build();

        DataStream<Comment> stream = env.addSource(new FlinkKafkaConsumer<>("flink", new CommentSchema(), properties))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Comment>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(Comment comment) {
                        return comment.createDate*1000;
                    }
                });

        DataStream<Comment> inputStream = stream.filter(x -> x.userID != 0);

        DataStream<Comment> originalComments = inputStream.filter(x -> x.depth == 1);

        /**
         * SALVATAGGIO COMMENTI DEPTH 1 SU REDIS
         * output: tuple(id comment, id autore)
         * redis sink, string set
         * salvataggio su redis delle informazioni (id commento / id autore) dei commenti depth 1
         */
        DataStreamSink<Tuple2<String, String>> commentAuthor = originalComments.map(new MapFunction<Comment, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(Comment comment) throws Exception {
                //id commento , id user
                return new Tuple2<>(Integer.toString(comment.commentID), Integer.toString(comment.userID));
            }
        }).addSink( new RedisSink<>(conf, new RedisDepthOne()));

        /**
         * commenti depth1
         * conteggio dei like
         * output: (autore, likes)
         * estraggo i like per ciascun commento
         */
        DataStream<Tuple2<String,Double>> directCommentsLikes = originalComments
                .map(new MapFunction<Comment, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(Comment comment) {

                        String author = Integer.toString(comment.userID);
                        Double likes = new Double(comment.recommendations);
                        if(comment.editorsSelection){
                            double increment = likes*0.1;
                            likes += increment;
                        }

                        return new Tuple2<>(author,likes*0.3);
                    }
                }).keyBy(0).timeWindow(Time.hours(window_dimension)).sum(1);

        /**
         * commenti DEPTH 2
         * output: tuple(id commento padre, id user padre, id user nonno
         * tuple pronte per essere salvate su redis
         */
        DataStream<Tuple3<String,String,String>> twoStream = inputStream.filter(x -> x.depth == 2).flatMap(new MapFunctionDepth2());

        /**
         * salvataggio su redis delle tuple depth 2
         */
        DataStreamSink<Tuple3<String, String, String>> sinkTwo = twoStream.addSink(new RedisSink<>(conf, new RedisDepthTwo()));

        /**
         * output: tuple(autore,count comm)
         * conteggio dei commenti per le tuple autore/count a partire dai commenti di depth 2
         */
        DataStream<Tuple2<String,Double>> commCountDepthTwo = twoStream.map(new MapFunction<Tuple3<String, String, String>, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(Tuple3<String, String, String> stringStringStringTuple3) throws Exception {
                return new Tuple2<>(stringStringStringTuple3.f2,1*0.7);
            }
        }).keyBy(0).timeWindow(Time.hours(window_dimension)).sum(1);

        /**
         * commenti DEPTH 3
         * output: tuple(autore,count comm)
         * conteggio dei commenti per le tuple autore/count a partire dai commenti di depth 3
         */
        DataStream<Tuple2<String, Double>> commCountDepthThree = inputStream.filter(x -> x.depth == 3)
                .flatMap(new MapFunctionDepth3())
                .keyBy(0)
                .timeWindow(Time.hours(window_dimension))
                .sum(1);


        /**
         * output: tuple(autore/count comm totale * peso)
         * union degli stream riguardanti i count dei commenti
         * valori finali per i commenti
         */
        DataStream<Tuple2<String, Double>> joinDepthTwoThree = commCountDepthTwo.union(commCountDepthThree)
                .keyBy(0).timeWindow(Time.hours(window_dimension))
                .sum(1);
        /**
         * output: tuple(autore/rank)
         * union e somma dei rank per commenti e like per ciascun user
         */
        DataStream<Tuple2<String, Double>> windowResults = directCommentsLikes.union(joinDepthTwoThree)
                .keyBy(0).timeWindow(Time.hours(window_dimension)).sum(1);

        /**
         * ranking
         * top 10
         */
        DataStream<String> resultRanking = windowResults.timeWindowAll(Time.hours(window_dimension))
                .apply(new AllWindowFunction<Tuple2<String, Double>, String, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Tuple2<String, Double>> iterable, Collector<String> collector) throws Exception {

                        List<Tuple2<String,Double>> myList = new ArrayList<>();
                        for(Tuple2<String,Double> t : iterable){
                            myList.add(t);
                        }

                        //String result = "" + new Date(timeWindow.getStart()) +"";
                        String result = "" + timeWindow.getStart() +"";

                        int i=0;
                        while(myList.size() > 0 && i < 10){

                            Tuple2<String, Double> t = getMaxTuple(myList);
                            result += ", "+t.f0+", "+t.f1;
                            myList.remove(t);
                            i++;

                        }

                        //System.out.println(result);

                        collector.collect(result);

                    }
                });

        resultRanking.writeAsText(file_path, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        env.execute();


    }

    private static Tuple2<String, Double> getMaxTuple(List<Tuple2<String, Double>> myList) {

        Tuple2<String, Double> maxTuple = myList.get(0);
        double max = myList.get(0).f1;

        for (Tuple2<String, Double> t : myList) {
            if(t.f1 > max){
                max = t.f1;
                maxTuple = t;
            }
        }

        return maxTuple;

    }


    public static class RedisDepthTwo implements RedisMapper<Tuple3<String,String,String>>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SET, "SET_NAME");
        }

        @Override
        public String getKeyFromData(Tuple3<String, String, String> stringStringStringTuple3) {
            return stringStringStringTuple3.f0;
        }

        @Override
        public String getValueFromData(Tuple3<String, String, String> stringStringStringTuple3) {
            String v = stringStringStringTuple3.f1 +"_"+stringStringStringTuple3.f2;
            return v;
        }
    }


    public static class RedisDepthOne implements RedisMapper<Tuple2<String, String>>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SET, "SET_NAME");
        }

        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data.f1;
        }
    }



}
