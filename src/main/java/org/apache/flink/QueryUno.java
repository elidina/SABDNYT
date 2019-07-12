package org.apache.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.entities.ArticleObject;
import org.apache.flink.entities.Comment;
import org.apache.flink.entities.CommentSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * QUERY 1
 */
//TODO: gestione stato backend / finestre temporali diverse / importo dati kafkaconsumer

public class QueryUno {


    public static void query1() throws Exception {

     //   final Logger logger  = LoggerFactory.getLogger(QueryUno.class);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final int daily_Window_size = 24;
        final int weekly_Window_size = 24*7;
        final int hourly_Window_size = 1;

        final int window_dimension = hourly_Window_size;

        String file_path = "query1_output_"+window_dimension+".txt";

        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
      /*  env.setParallelism(8);
        env.setRestartStrategy(RestartStrategies.noRestart());

        env.setStateBackend(new RocksDBStateBackend("file:///tmp"));
        */

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "flink");

        env.getConfig().setLatencyTrackingInterval(10000);

        DataStream<Comment> inputStream = env.addSource(new FlinkKafkaConsumer<>("flink", new CommentSchema(), properties))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Comment>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(Comment comment) {
                        return comment.createDate*1000;
                    }
                }).filter(x -> x.userID != 0);


        DataStream<ArticleObject> windowCounts = inputStream.flatMap(new FlatMapFunction<Comment, ArticleObject>() {
                    @Override
                    public void flatMap(Comment s, Collector<ArticleObject> collector) throws Exception {
                        //String[] str = s.split(",");

                        ArticleObject ao = new ArticleObject(s.articleID, 1,s.createDate);
                        //System.out.println(ao);
                        collector.collect(ao);

                    }
                }).keyBy("article").timeWindow(Time.hours(window_dimension)).reduce(new ReduceFunction<ArticleObject>() {
                    @Override
                    public ArticleObject reduce(ArticleObject a1, ArticleObject a2) throws Exception {
                        return new ArticleObject(a1.article, a1.comment + a2.comment, Calendar.getInstance().getTimeInMillis());
                    }
                });


        DataStream<String> resultList = windowCounts.timeWindowAll(Time.hours(window_dimension)).apply(new AllWindowFunction<ArticleObject, String, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<ArticleObject> iterable, Collector<String> collector) throws Exception {
                List<ArticleObject> myList = new ArrayList<>();
                for(ArticleObject t : iterable){
                    myList.add(t);
                }

                Collections.sort(myList, new Comparator<ArticleObject>() {
                    @Override
                    public int compare(ArticleObject o1, ArticleObject o2) {

                        int first = o1.comment;
                        int second = o2.comment;
                        return first - second;
                    }
                });


                String result = new Date(timeWindow.getStart())+ "";
                for (int i = 0; i < 3; i++) {
                    result += ", "+ myList.get(i).article + ", "+myList.get(i).comment;

                }

                collector.collect(result);
            }
        });

/*
        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
        resultList.transform("DummyLatencySink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
                .setParallelism(1).name("Latency Sink")
                .uid("Latency-Sink");
                */

        //resultList.print().setParallelism(1);

        //esultList.writeAsText(file_path, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("Query1");
    }

    private static ArticleObject getMaxArticle(List<ArticleObject> myList) {

        ArticleObject maxArticle = null;
        Integer max = 0;

        for (ArticleObject t : myList) {
            if(t.comment > max){
                max = t.comment;
                maxArticle = t;
            }
        }

        return maxArticle;

    }


    public static List<ArticleObject> getTop3Articles(List<ArticleObject> resultList){

        ArticleObject firstArticle = getNextTop(resultList);
        resultList.remove(firstArticle);
        ArticleObject secondArticle = getNextTop(resultList);
        resultList.remove(secondArticle);
        ArticleObject thirdArticle = getNextTop(resultList);
        resultList.remove(thirdArticle);

        List<ArticleObject> top3List = new ArrayList<>();
        top3List.add(firstArticle);
        top3List.add(secondArticle);
        top3List.add(thirdArticle);

        return top3List;
    }

    public static ArticleObject getNextTop(List<ArticleObject> list){

        int max =0;
        ArticleObject best = null;

        for(ArticleObject ac : list){
            if(ac.comment > max){
                best = ac;
                max = best.comment;
            }
        }

        return best;

    }

    /*

    public static class DummyLatencyCountingSink<T> extends StreamSink<T> {

        private final Logger logger;

        public DummyLatencyCountingSink(Logger log) {
            super(new SinkFunction<T>() {

                @Override
                public void invoke(T value, Context ctx) throws Exception {}
            });
            logger = log;
        }

        @Override
        public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {

            Long rn = System.currentTimeMillis();
            System.out.println("*****");
            System.out.println(rn - latencyMarker.getMarkedTime());
            System.out.println("current time:"+ rn+" - latencyMarker: "+ latencyMarker.getMarkedTime());
            logger.warn("%{}%{}%{}%{}%{}%{}", "latency",
                    System.currentTimeMillis() - latencyMarker.getMarkedTime(), System.currentTimeMillis(), latencyMarker.getMarkedTime(),
                    latencyMarker.getSubtaskIndex(), getRuntimeContext().getIndexOfThisSubtask());
        }
    }


*/

}
