package org.apache.flink.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.entities.Comment;
import org.apache.flink.entities.CommentQ2;
import org.apache.flink.entities.CommentSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;

import static org.apache.flink.utils.TimestampHandler.calcolaIndex;
import static org.apache.flink.utils.TimestampHandler.estraiGiorno;

public class QueryDueKeyTest2 {

    public static void main(String[] args) throws Exception {

        final int daily_Window_size = 24;
        final int weekly_Window_size = 24 * 7;
        final int monthly_Window_size = 24 * 30;

        final int window_dimension = daily_Window_size;

        String file_path = "query2_output_" + window_dimension + ".txt";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "flink");

        DataStream<Comment> inputStream = env.addSource(new FlinkKafkaConsumer<>("flink", new CommentSchema(), properties))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Comment>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(Comment s) {

                        //System.out.println(s.createDate * 1000);
                        return s.createDate * 1000;
                    }
                })
                .filter(x -> x.userID != 0 && x.commentType.contains("comment"));

        DataStream<String> resultList = inputStream.map(new MapFunction<Comment, Tuple4<String, Integer, Long, Integer>>() {
            @Override
            public Tuple4<String, Integer, Long, Integer> map(Comment comment) throws Exception {

                int index = calcolaIndex(comment.createDate*1000);


                return new Tuple4<>(comment.articleID, 1, comment.arrivalTime, index);
            }
        }).keyBy(0).timeWindow(Time.hours(2)).reduce(new ReduceFunction<Tuple4<String, Integer, Long, Integer>>() {
            @Override
            public Tuple4<String, Integer, Long, Integer> reduce(Tuple4<String, Integer, Long, Integer> t1, Tuple4<String, Integer, Long, Integer> t2) throws Exception {

                long v = 0;
                if(t1.f2 > t2.f2){
                    v = t1.f2;
                }else{
                    v = t2.f2;
                }

                return new Tuple4<>(t1.f0, t1.f1+t2.f1,v ,t1.f3);
            }
        }).map(new MapFunction<Tuple4<String, Integer, Long, Integer>, Tuple3<Integer, Long, Integer>>() {
            @Override
            public Tuple3<Integer, Long, Integer> map(Tuple4<String, Integer, Long, Integer> t) throws Exception {

                //indice, ts, count
                return new Tuple3<>(t.f3, t.f2, t.f1);
            }
        }).timeWindowAll(Time.hours(2)).reduce(new ReduceFunction<Tuple3<Integer, Long, Integer>>() {
            @Override
            public Tuple3<Integer, Long, Integer> reduce(Tuple3<Integer, Long, Integer> t1, Tuple3<Integer, Long, Integer> t2) throws Exception {

                long v = 0;
                if(t1.f1 > t2.f1){
                    v = t1.f1;
                }else{
                    v = t2.f1;
                }

                return new Tuple3<>(t1.f0,v ,t1.f2 + t2.f2);
            }
        }).keyBy(0).timeWindow(Time.hours(window_dimension)).reduce(new ReduceFunction<Tuple3<Integer, Long, Integer>>() {
            @Override
            public Tuple3<Integer, Long, Integer> reduce(Tuple3<Integer, Long, Integer> t1, Tuple3<Integer, Long, Integer> t2) throws Exception {

                long v = 0;
                if(t1.f1 > t2.f1){
                    v = t1.f1;
                }else{
                    v = t2.f1;
                }

                return new Tuple3<>(t1.f0, v, t1.f2+t2.f2);
            }
        }).timeWindowAll(Time.hours(window_dimension)).apply(new AllWindowFunction<Tuple3<Integer, Long, Integer>, String, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<Tuple3<Integer, Long, Integer>> iterable, Collector<String> collector) throws Exception {

                String result = new Date(timeWindow.getStart())+"";

                Long oldestAT = 0L;

                for (int i = 1; i < 13; i++) {

                    boolean done = false;
                    for(Tuple3<Integer, Long, Integer> t : iterable){

                        if(t.f0 == i){
                            done = true;
                            result += ", "+t.f2;
                        }

                        if(t.f1 > oldestAT){
                            oldestAT = t.f1;
                        }

                    }
                    if(!done){
                        result += ", 0";
                    }

                }

                collector.collect(result);

                Long current = System.currentTimeMillis();
                Long diff = current-oldestAT;

                System.out.println(diff);


            }
        });


        env.execute("Query2Key_Test");

    }


}
