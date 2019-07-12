package org.apache.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.entities.Comment;
import org.apache.flink.entities.CommentSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.utils.TimestampHandler.calcolaIndex;

public class QueryDue {



    public static void main(String[] args) throws Exception {

        final Integer[] windowCountList = {0,0,0,0,0,0,0,0,0,0,0,0};

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



        DataStream<String> result = inputStream.map(new MapFunction<Comment, Tuple2<Integer, Long>>() {
            @Override
            public Tuple2<Integer, Long> map(Comment comment) throws Exception {
                return new Tuple2<>(1,comment.createDate*1000);
            }
        }).windowAll(GlobalWindows.create()).trigger(new Trigger<Tuple2<Integer, Long>, GlobalWindow>() {

            private Long next_fire;
            private Long last_fire;
            private Integer window_dimension = 24*7;
            private Integer dimension = 86400000;

            private Integer last_elem_index = 0;
            private Long ts_first_elem = 0L;

            private Integer boolElem;

            private List<Integer> listCount;

            @Override
            public TriggerResult onElement(Tuple2<Integer, Long> t, long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
                long ts = t.f1;
                int index = calcolaIndex(t.f1*1000);

                if(last_fire==null){
                    last_fire = 1514761200000L;
                }

                if(listCount==null){
                    listCount = new ArrayList<>();
                    for (int i = 0; i < 12; i++) {
                        listCount.add(0);
                    }
                }

                if(boolElem==null){
                    boolElem=0;
                }

                if(ts - last_fire >= dimension){ //nuovo elemento appartiene alla finestra successiva. lo conservo e faccio la fire&purge.

                    System.out.println("*** diff "+(ts - last_fire));

                    last_fire = last_fire + dimension;
                    ts_first_elem = ts;
                    next_fire = ts + dimension;
                    boolElem = 1;


                    String res ="";
                    for (int i = 0; i <12 ; i++) {
                        res += ", " + listCount.get(i);
                    }

                    for (int i = 0; i < 12 ; i++) {
                        windowCountList[i] = listCount.get(i);
                        listCount.set(i,0);
                    }
                    return TriggerResult.FIRE_AND_PURGE;

                }

                listCount.set(index-1, listCount.get(index-1) +1);

                if(ts_first_elem - last_fire < dimension){
                    if(boolElem == 1){
                        //incrementa suo contatore
                        int new_index = calcolaIndex(ts_first_elem);

                        listCount.set(new_index-1, listCount.get(new_index-1) +1);

                        boolElem=0;

                    }
                }

                return TriggerResult.CONTINUE;
            }

            @Override
            public TriggerResult onProcessingTime(long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
                return null;
            }

            @Override
            public TriggerResult onEventTime(long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
                return null;
            }

            @Override
            public void clear(GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {

            }
        }).apply(new AllWindowFunction<Tuple2<Integer, Long>, String, GlobalWindow>() {
            @Override
            public void apply(GlobalWindow globalWindow, Iterable<Tuple2<Integer, Long>> iterable, Collector<String> collector) throws Exception {

                String res = "";
                for(Integer numb : windowCountList){
                    res += ", "+numb;
                }

                System.out.println(res);

            }
        });

        result.print().setParallelism(1);


        env.execute();
    }


}
