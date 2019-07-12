package org.apache.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.core.fs.FileSystem;
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

/**
 * Query 2 implementata utilizzando un'unica finestra tumbling Flink
 * i dati in entrata sono separati per chiave (fascia oraria) ed aggregati.
 */

public class QueryDueKey {

    public static void main(String[] args) throws Exception {

        final int daily_Window_size = 24;
        final int weekly_Window_size = 24*7;
        final int monthly_Window_size = 24*30;

        final int window_dimension = daily_Window_size;

        String file_path = "query2_output_"+window_dimension+".txt";

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


        DataStream<CommentQ2> windowCounts = inputStream.flatMap(new FlatMapFunction<Comment, CommentQ2>() {
            @Override
            public void flatMap(Comment c, Collector<CommentQ2> collector) throws Exception {

                int index = calcolaIndex(c.createDate*1000);
                int day = estraiGiorno(c.createDate*1000);

                TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
                Calendar calendar = Calendar.getInstance();
                calendar.setTimeInMillis(c.createDate*1000);

                Date date = calendar.getTime();

                CommentQ2 comment = new CommentQ2(c.createDate*1000, c.commentType, 1, index, day);
                collector.collect(comment);
            }
        }).keyBy("index")
                .timeWindow(Time.hours(24))
                .reduce(new ReduceFunction<CommentQ2>() {
                    @Override
                    public CommentQ2 reduce(CommentQ2 commentQ2, CommentQ2 t1) throws Exception {
                        return new CommentQ2(commentQ2.getTs(), t1.getType(),
                                commentQ2.getCount() + t1.getCount(), commentQ2.getIndex(), commentQ2.getDay());
                    }
                });

                DataStream<String> resultList = windowCounts
                        .timeWindowAll(Time.hours(daily_Window_size))
                        .apply(new AllWindowFunction<CommentQ2, String, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow timeWindow, Iterable<CommentQ2> iterable, Collector<String> collector) throws Exception {

                                String result = new Date(timeWindow.getStart())+"";

                                for (int i = 1; i < 13; i++) {
                                    boolean done = false;
                                    for(CommentQ2 c : iterable){
                                        if(c.getIndex() == i){
                                            done = true;
                                            //result += ", "+ ((i-1)*2)+"-"+ ((i*2)-1)+": "+c.getCount();
                                            result += ", "+c.getCount();
                                        }
                                    }
                                    if(!done){
                                        result += ", 0";
                                    }

                                }

                                System.out.println(result);

                                collector.collect(result);


                            }
                        });


        resultList.writeAsText(file_path, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

    }
}
