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

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        /*
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(8);
        env.setRestartStrategy(RestartStrategies.noRestart());

        env.setStateBackend(new RocksDBStateBackend("file:///tmp"));
        */

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "flink");

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").build();

        DataStream<CommentLog> inputStream = env.addSource(new FlinkKafkaConsumer<>("flink", new CommentLogSchema(), properties))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<CommentLog>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(CommentLog commentLog) {
                        return commentLog.createDate*1000;
                    }
                }).filter(x -> x.userID != 0);

        DataStream<CommentLog> originalComments = inputStream.filter(x -> x.depth == 1);

        /**
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
                }).keyBy(0).timeWindow(Time.seconds(30*60)).sum(1);

        /**
         * steam depth 2
         * output: tuple(id commento padre, id user padre, id user nonno
         * tuple pronte per essere salvate su redis
         */
        DataStream<Tuple3<String,String,String>> twoStream = inputStream.filter(x -> x.depth == 2).flatMap(new FlatMapFunction<CommentLog, Tuple3<String, String, String>>() {
            @Override
            public void flatMap(CommentLog commentLog, Collector<Tuple3<String, String, String>> collector) throws Exception {
                Jedis jedis = new Jedis();
                String value = jedis.get(Integer.toString(commentLog.inReplyTo));

                if(value != null){
                    //id commento padre, id user padre, id autore nonno
                    Tuple3<String,String,String> t = new Tuple3<>(Integer.toString(commentLog.commentID), Integer.toString(commentLog.userID), value);

                    collector.collect(t);
                }

            }
        });

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
        }).keyBy(0).timeWindow(Time.seconds(30*60)).sum(1);


        /**
         * output: tuple(autore,count comm)
         * calcola le tuple autore/count per i commenti di depth 3
         */
        DataStream<Tuple2<String, Double>> commCountDepthThree = inputStream.filter(x -> x.depth == 3).flatMap(new FlatMapFunction<CommentLog, Tuple2<String, Double>>() {
            @Override
            public void flatMap(CommentLog commentLog, Collector<Tuple2<String, Double>> collector) throws Exception {
                String commentoPadre = Integer.toString(commentLog.inReplyTo);
                Jedis jedisThree = new Jedis();
                String[] values = jedisThree.get(commentoPadre).split("_");

                if(values[0] != null){
                    Tuple2<String, Double> t1 = new Tuple2<>(values[0],1*0.7);
                    collector.collect(t1);

                }
                if(values[1] != null){
                    Tuple2<String, Double> t2 = new Tuple2<>(values[1],1*0.7);
                    collector.collect(t2);


                }


            }
        }).keyBy(0).timeWindow(Time.seconds(30*60)).sum(1);

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
        DataStream<String> resultRanking = windowResults.timeWindowAll(Time.seconds(30*60))
                .apply(new AllWindowFunction<Tuple2<String, Double>, String, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Tuple2<String, Double>> iterable, Collector<String> collector) throws Exception {

                        List<Tuple2<String,Double>> myList = new ArrayList<>();
                        for(Tuple2<String,Double> t : iterable){
                            myList.add(t);
                        }

                        String result = "" + new Date(timeWindow.getStart()) +": ";

                        for (int i = 0; i < 10 && i < myList.size(); i++) {

                            if(i >1){
                                result = result + ", ";
                            }

                            Tuple2<String,Double> max = getMaxTuple(myList);
                            myList.remove(max);
                            result = "user id: " + max.f0 + " - ranking: "+ max.f1;
                        }

                        System.out.println("******** RANKING ********\n\n" + result);

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
