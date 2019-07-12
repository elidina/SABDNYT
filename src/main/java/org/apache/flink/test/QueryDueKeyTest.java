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

public class QueryDueKeyTest {

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

        DataStream<CommentQ2> windowCounts = inputStream.map(new MapFunction<Comment, CommentQ2>() {
            @Override
            public CommentQ2 map(Comment comment) throws Exception {

                int index = calcolaIndex(comment.createDate*1000);
                int day = estraiGiorno(comment.createDate*1000);

                CommentQ2 c = new CommentQ2(comment.createDate*1000, comment.commentType, 1, index, day, comment.arrivalTime);

                return c;
            }
        }).keyBy("index").timeWindow(Time.hours(window_dimension)).reduce(new ReduceFunction<CommentQ2>() {
            @Override
            public CommentQ2 reduce(CommentQ2 c1, CommentQ2 c2) throws Exception {


                long v = 0;
                if(c1.getArrivalTime() > c2.getArrivalTime()){
                    v = c1.getArrivalTime();
                }else{
                    v = c2.getArrivalTime();
                }

                return new CommentQ2(c1.getTs(), c1.getType(),
                        c1.getCount() + c2.getCount(), c1.getIndex(), c1.getDay(),v);            }
        });


        DataStream<String> result = windowCounts.timeWindowAll(Time.hours(window_dimension)).apply(new AllWindowFunction<CommentQ2, String, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<CommentQ2> iterable, Collector<String> collector) throws Exception {

                String result = new Date(timeWindow.getStart())+"";

                Long oldestAT = 0L;

                for (int i = 1; i < 13; i++) {
                    System.out.println("*");

                    boolean done = false;
                    for(CommentQ2 c : iterable){

                        if(c.getIndex() == i){
                            done = true;
                            result += ", "+c.getCount();
                        }

                        if(c.getArrivalTime() > oldestAT){
                            oldestAT = c.getArrivalTime();
                        }

                    }
                    if(!done){
                        result += ", 0";
                    }

                }

                System.out.println(result);

                collector.collect(result);

                Long current = System.currentTimeMillis();
                Long diff = current-oldestAT;

                System.out.println(diff);


            }
        });

        env.execute("Query2Key_Test");

    }


}
