package org.apache.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
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

public class QueryOne {


    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "flink2");


        DataStream<ArticleObject> windowCounts = env.addSource(new FlinkKafkaConsumer<>("flink2", new SimpleStringSchema(), properties))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(1)) {
                    //assegno timestamp (createdate)
                    @Override
                    public long extractTimestamp(String s) {
                        String[] entry = s.split(",");
                        return Long.parseLong(entry[5]);
                    }
                }).flatMap(new FlatMapFunction<String, ArticleObject>() {
                    @Override
                    //creazione oggetto ArticleObject che contiene articleID e contatore a 1
                    public void flatMap(String s, Collector<ArticleObject> collector) throws Exception {
                        String[] str = s.split(",");

                        ArticleObject ao = new ArticleObject(str[1], 1,Long.parseLong(str[0]));
                        collector.collect(ao);

                    }
                }).keyBy("article").timeWindow(Time.seconds(10), Time.seconds(5)).reduce(new ReduceFunction<ArticleObject>() {
                    @Override
                    //somma dei count per articleid
                    public ArticleObject reduce(ArticleObject a1, ArticleObject a2) throws Exception {
                        return new ArticleObject(a1.article, a1.comment + a2.comment, Calendar.getInstance().getTimeInMillis());
                    }
                });

        DataStream<String> resultList = windowCounts.map(new MapFunction<ArticleObject, Tuple2<String, ArticleObject>>() {
            @Override
            //map per cambiare chiave
            public Tuple2<String, ArticleObject> map(ArticleObject articleObject) throws Exception {
                return new Tuple2<>("label", articleObject);
            }
        }).keyBy(0).timeWindow(Time.seconds(5)).apply(new WindowFunction<Tuple2<String, ArticleObject>, String, Tuple, TimeWindow>() {
            @Override
            //classifica
            public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, ArticleObject>> iterable, Collector<String> collector) throws Exception {
                List<ArticleObject> myList = new ArrayList<>();
                for(Tuple2<String, ArticleObject> t : iterable){
                    myList.add(t.f1);
                }

                String one = "";
                String two = "";
                String three = "";

                List<ArticleObject> results = getTop3Articles(myList);
                if(results.get(0) != null){
                    one= results.get(0).toString();
                }
                if(results.get(1) != null){
                    two= results.get(1).toString();
                }
                if(results.get(2) != null){
                    three= results.get(2).toString();
                }

                Long time = timeWindow.getStart();
                String finalRes = "Time: "+time+" \nFirst: "+one+" Second: "+two+" Third: "+three;

                collector.collect(finalRes);

            }
        });

        //resultList.print().setParallelism(1);

        resultList.writeAsText("query1output.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("Query1");
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



}
