package org.apache.flink.test;

import org.apache.flink.entities.Comment;
import org.apache.flink.entities.CommentSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.*;

public class QueryUnoTest {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final int daily_Window_size = 24;
        final int weekly_Window_size = 24 * 7;
        final int hourly_Window_size = 1;

        final int window_dimension = hourly_Window_size;

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "flink");


        DataStream<Comment> inputStream = env.addSource(new FlinkKafkaConsumer<>("flink", new CommentSchema(), properties))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Comment>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(Comment comment) {
                        return comment.createDate * 1000;
                    }
                }).filter(x -> x.userID != 0);

        DataStream<Tuple3<String, Integer, Long>> boh = inputStream.map(new MapFunction<Comment, Tuple3<String, Integer, Long>>() {
            @Override
            public Tuple3<String, Integer, Long> map(Comment comment) throws Exception {
                return new Tuple3<>(comment.articleID,1,comment.arrivalTime);
            }
        }).keyBy(0).timeWindow(Time.hours(window_dimension)).reduce(new ReduceFunction<Tuple3<String, Integer, Long>>() {
            @Override
            public Tuple3<String, Integer, Long> reduce(Tuple3<String, Integer, Long> a1, Tuple3<String, Integer, Long> a2) throws Exception {

                long res = 0;
                if(a1.f2 > a2.f2){
                    res = a1.f2;
                }else{
                    res = a2.f2;
                }

                return new Tuple3<>(a1.f0, a1.f1 + a2.f1, res);
            }
        });

        DataStream<String> rest = boh.timeWindowAll(Time.hours(window_dimension)).apply(new AllWindowFunction<Tuple3<String, Integer, Long>, String, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<Tuple3<String, Integer, Long>> iterable, Collector<String> collector) throws Exception {

                List<Tuple3<String, Integer, Long>> myList = new ArrayList<>();
                for(Tuple3<String, Integer, Long> t : iterable){
                    myList.add(t);
                }


                Collections.sort(myList, new Comparator<Tuple3<String, Integer, Long>>() {
                    @Override
                    public int compare(Tuple3<String, Integer, Long> o1, Tuple3<String, Integer, Long> o2) {
                        int first = o1.f1;
                        int second = o2.f1;
                        return first - second;
                    }
                });



                String result = new Date(timeWindow.getStart())+ "";
                for (int i = 0; i < 3 && i < myList.size(); i++) {
                    result += ", "+ myList.get(i).f0 + ", "+myList.get(i).f1;
                }

                collector.collect(result);

                long current = System.currentTimeMillis();
                long start = 0;

                if(myList.size() > 0){
                    for (int i = 0; i < myList.size() ; i++) {
                        if(myList.get(i).f2 > start){
                            start = myList.get(i).f2;
                        }
                    }
                }


                long diff = current - start;
                System.out.println(diff);

            }
        });

        //rest.writeAsText("provatest.txt",FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        env.execute();

    }


    public static List<Tuple3<String, Integer, Long>> getTopThree(List<Tuple3<String, Integer, Long>> list){

        List<Tuple3<String, Integer, Long>> newList = new ArrayList<>();

        Tuple3<String, Integer, Long> one = null;
        Tuple3<String, Integer, Long> two = null;
        Tuple3<String, Integer, Long> three = null;

        Integer one_max = 0; // I
        Integer two_max = 0; // II
        Integer three_max = 0; // III

        for (int i = 0; i < 3 && i < list.size(); i++) {

            Tuple3<String, Integer, Long> t = list.get(i);

            if(t.f1 > one_max){ //A > I
                three_max = two_max;
                two_max = one_max;
                one_max = t.f1;

                three = two;
                two = one;
                one = t;

            }else if((t.f1 > two_max && t.f1 < one_max) || (t.f1 == one_max)){ // II < A < I or A  == I
                three_max = two_max;
                two_max =t.f1;

                three = two;
                two = t;

            }else if((t.f1 > three_max && t.f1 < two_max) || (t.f1 == two_max)){ // III < A < II or A == II
                three_max = t.f1;

                three = t;
            }

        }

        newList.add(one);
        newList.add(two);
        newList.add(three);
        return newList;


    }

}
