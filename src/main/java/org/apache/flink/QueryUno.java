package org.apache.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.entities.ArticleObject;
import org.apache.flink.entities.Comment;
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

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;

/**
 * QUERY 1
 */
//TODO: gestione stato backend / finestre temporali diverse / importo dati kafkaconsumer

public class QueryUno {


    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final int daily_Window_size = 24;
        final int weekly_Window_size = 24*7;
        final int hourly_Window_size = 1;

        final int window_dimension = hourly_Window_size;

        String file_path = "query1_output_"+window_dimension+".txt";

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "flink");

        //stream di input, assegnazione del timestamp
        DataStream<Comment> inputStream = env.addSource(new FlinkKafkaConsumer<>("flink", new CommentSchema(), properties))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Comment>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(Comment comment) {
                        return comment.createDate*1000;
                    }
                }).filter(x -> x.userID != 0);


        //i Commenti sono mappati in oggetti ArticleObject, contenenti l'id dell'articolo e un counter settato ad 1.
        //gli elementi sono divisi per chiave (articleID) e contati.
        DataStream<ArticleObject> windowCounts = inputStream.map(new MapFunction<Comment, ArticleObject>() {
            @Override
            public ArticleObject map(Comment comment) throws Exception {
                ArticleObject ao = new ArticleObject(comment.articleID, 1,comment.createDate);

                return ao;
            }
        }).keyBy("article").timeWindow(Time.hours(window_dimension)).reduce(new ReduceFunction<ArticleObject>() {
                    @Override
                    public ArticleObject reduce(ArticleObject a1, ArticleObject a2) throws Exception {
                        return new ArticleObject(a1.article, a1.comment + a2.comment, Calendar.getInstance().getTimeInMillis());
                    }
                });


        //gli elementi sono raggruppati e poi viene stilata la classifica
        DataStream<String> resultList = windowCounts.timeWindowAll(Time.hours(window_dimension)).apply(new AllWindowFunction<ArticleObject, String, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<ArticleObject> iterable, Collector<String> collector) throws Exception {
                List<ArticleObject> myList = new ArrayList<>();
                for(ArticleObject t : iterable){
                    myList.add(t);
                }

                String result = timeWindow.getStart()+ "";
                //String result = new Date(timeWindow.getStart())+ "";

                int i=0;
                while(myList.size() > 0 && i < 3){

                    ArticleObject ao = getMaxArticle(myList);
                    result += ", "+ao.article+", "+ao.comment;
                    myList.remove(ao);
                    i++;

                }

                //System.out.println(result);

                collector.collect(result);
            }
        });

        //resultList.print().setParallelism(1);

        resultList.writeAsText(file_path, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("Query1");
    }


    private static ArticleObject getMaxArticle(List<ArticleObject> myList) {

        ArticleObject maxArticle = myList.get(0);
        Integer max = myList.get(0).comment;

        for (ArticleObject t : myList) {
            if(t.comment > max){
                max = t.comment;
                maxArticle = t;
            }
        }

        return maxArticle;

    }


}
