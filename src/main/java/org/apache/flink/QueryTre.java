package org.apache.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
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
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class QueryTre {

    public static void main(String[] args) throws Exception {

        final int DAILY_WINDOW_SIZE = 24;
        final int WEEKLY_WINDOW_SIZE = DAILY_WINDOW_SIZE*7;
        final int MONTHLY_WINDOW_SIZE = WEEKLY_WINDOW_SIZE*4;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "flink5");

        JedisPoolFactory.init("localhost", 6379);

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").build();

/*
        Jedis jedis = new Jedis();
        jedis.flushAll();
        jedis.close();
*/

        DataStream<CommentLog> inputStream = env.addSource(new FlinkKafkaConsumer<>("flink5", new CommentLogSchema(), properties))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<CommentLog>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(CommentLog commentLog) {
                        return commentLog.createDate*1000;
                    }
                }).filter(x -> x.userID != 0);

        DataStream<CommentLog> originalComments = inputStream.filter(x -> x.depth == 1);

        /**
         * SALVATAGGIO COMMENTI DEPTH 1 SU REDIS
         * output: tuple(id comment, id autore)
         * redis sink, string set
         * salvataggio su redis delle informazioni (id commento / id autore) dei commenti depth 1
         */
        DataStreamSink<Tuple2<String, String>> commentAuthor = originalComments.map(new MapFunction<CommentLog, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(CommentLog commentLog) throws Exception {
                //id commento , id user
                Tuple2<String,String> commentAuthor = new Tuple2<>(Integer.toString(commentLog.commentID), Integer.toString(commentLog.userID));
                return commentAuthor;
            }
        }).addSink( new RedisSink<>(conf, new RedisDepthOne()));

        /**
         * output: (autore, likes)
         * estraggo i like per ciascun commento
         */
        DataStream<Tuple2<String,Double>> directCommentsLikes = originalComments
                .map(new MapFunction<CommentLog, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(CommentLog commentLog) {

                        String author = Integer.toString(commentLog.userID);
                        Double likes = new Double(commentLog.recommendations);
                        if(commentLog.editorsSelection){
                            int increment = (int) (likes*0.1);
                            likes += increment;
                        }

                        return new Tuple2<>(author,likes*0.3);
                    }
                }).keyBy(0).timeWindow(Time.hours(DAILY_WINDOW_SIZE)).sum(1);

        /**
         * DEPTH 2
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
         * calcola le tuple autore/count per i commenti di depth 2
         */
        DataStream<Tuple2<String,Double>> commCountDepthTwo = twoStream.map(new MapFunction<Tuple3<String, String, String>, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(Tuple3<String, String, String> stringStringStringTuple3) throws Exception {
                return new Tuple2<>(stringStringStringTuple3.f2,1*0.7);
            }
        }).keyBy(0).timeWindow(Time.hours(DAILY_WINDOW_SIZE)).sum(1);


        /**
         * DEPTH 3
         * output: tuple(autore,count comm)
         * calcola le tuple autore/count per i commenti di depth 3
         */
        DataStream<Tuple2<String, Double>> commCountDepthThree = inputStream.filter(x -> x.depth == 3)
                .flatMap(new MapFunctionDepth3())
                .keyBy(0)
                .timeWindow(Time.seconds(DAILY_WINDOW_SIZE))
                .sum(1);

        /**
         * output: tuple(autore/count comm totale * peso)
         * valori finali per i commenti
         */
        DataStream<Tuple2<String, Double>> joinDepthTwoThree = commCountDepthTwo.union(commCountDepthThree)
                .keyBy(0)
                .sum(1);

        /**
         * output: tuple(autore/rank)
         * somma dei rank per commenti e like per ciascun user
         */
        DataStream<Tuple2<String, Double>> windowResults = directCommentsLikes.union(joinDepthTwoThree)
                .keyBy(0).sum(1);


        /**
         * ranking
         * top 10
         */
        DataStream<String> resultRanking = windowResults.timeWindowAll(Time.hours(DAILY_WINDOW_SIZE))
                .apply(new AllWindowFunction<Tuple2<String, Double>, String, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Tuple2<String, Double>> iterable, Collector<String> collector) throws Exception {

                        List<Tuple2<String,Double>> myList = new ArrayList<>();
                        for(Tuple2<String,Double> t : iterable){
                            myList.add(t);
                        }

                        String result = "" + new Date(timeWindow.getStart()) +": ";

                        for (int i = 0; i < 10 && i < myList.size(); i++) {

                            Tuple2<String,Double> max = getMaxTuple(myList);
                            myList.remove(max);
                            result = result + ", user id: " + max.f0 + " - ranking: "+ max.f1;
                        }

                        System.out.println(result+"\n\n");

                        collector.collect(result);

                    }
                });

        //resultRanking.print().setParallelism(1);

        resultRanking.writeAsText("outputquery3.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        env.execute();


    }

    private static Tuple2<String,Double> getMaxTuple(List<Tuple2<String, Double>> myList) {

        Tuple2<String,Double> maxTuple = null;
        Double max = 0.0;

        for (Tuple2<String,Double> t : myList) {
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
