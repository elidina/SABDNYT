package org.apache.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
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

import java.util.*;

import static org.apache.flink.utils.TimestampHandler.calcolaIndex;

/**
 * Implementazione della Query Due utilizzando un trigger custom su un'unica finestra.
 * Nella finestra vengono incremententi i counter per ciascuna fascia oraria, fino allo scatto della finestra.
 */

public class QueryDue {

    public static void main(String[] args) throws Exception {

        final Long[] windowCountList = {0L,0L,0L,0L,0L,0L,0L,0L,0L,0L,0L,0L,0L};

        final int daily_Window_size = 24;
        final int weekly_Window_size = 24 * 7;
        final int monthly_Window_size = 24 * 30;

        final int window_dimension = daily_Window_size;

        String file_path = "query2_output_trigger_" + window_dimension + ".txt";

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

            private Long last_fire;
            private Integer dimension = 86400000;   //un giorno
            //private Integer dimension = 86400000*7;  //settimana
            //private Long dimension = 2678400000L;  //mese

            private Integer last_elem_index = 0;
            private Long ts_first_elem = 0L;

            private Integer boolElem;

            private List<Long> listCount;
            private List<Long> tsElemScartati;

            @Override
            public TriggerResult onElement(Tuple2<Integer, Long> t, long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {

                long ts = t.f1;
                int index = calcolaIndex(t.f1);

                //System.out.println(ts + " - "+ new Date(ts) + " - "+index);

                if(last_fire==null){
                    last_fire = 1514761200000L;
                }

                if(listCount==null){
                    listCount = new ArrayList<>();
                    for (int i = 0; i < 13; i++) {
                        listCount.add(0L);
                    }
                }

                if(tsElemScartati==null){
                    tsElemScartati = new ArrayList<>();
                }

                if(boolElem==null){
                    boolElem=0;
                }

                /*
                Controllo l'elemento che ha fatto scattare la finestra precedente, (boolElem == 1), e tutti gli altri elementi
                da recuperare da scatti precedenti.
                Nel caso l'elemento rientri nella finestra corrente, lo processo.
                altrimenti, scatta trigger fire and purge (e salvo il dato arrivato  nel frattempo).
                 */
                if(boolElem == 1){

                    for (int i = 0; i < tsElemScartati.size(); i++) {

                        if(tsElemScartati.get(i) - last_fire < dimension){
                            int new_index = calcolaIndex(ts_first_elem);

                            listCount.set(new_index, listCount.get(new_index) +1);
                            tsElemScartati.remove(tsElemScartati.get(i));

                        }else{
                            //salvo l'ultimo elemento arrivato
                            tsElemScartati.add(ts);

                            last_fire = last_fire + dimension;

                            for (int j = 0; j < 13 ; j++) {
                                windowCountList[j] = listCount.get(j);
                                listCount.set(j,0L);
                            }

                            return TriggerResult.FIRE_AND_PURGE;
                        }

                    }

                    boolElem = 0;
                }

                if(ts - last_fire >= dimension){ //nuovo elemento appartiene alla finestra successiva. lo conservo e scatta il trigger la fire&purge.

                    //System.out.println("*** diff "+(ts - last_fire));

                    listCount.set(0, last_fire);

                    last_fire = last_fire + dimension;
                    ts_first_elem = ts;
                    //next_fire = ts + dimension;
                    boolElem = 1;

                    //salvo elemento scartato che ha fatto scattare la finestra
                    tsElemScartati.add(ts);

                    //svuoto la lista locale e sposto i risultati nella variabile globale
                    for (int i = 0; i < 13 ; i++) {
                        windowCountList[i] = listCount.get(i);
                        listCount.set(i,0L);
                    }

                    return TriggerResult.FIRE_AND_PURGE;

                }

                //incremento counter realativo all'ultimo elemento entrato nella finestra
                listCount.set(index, listCount.get(index) +1);

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


                //String res = ""+new Date(windowCountList[0]);

                String res = ""+windowCountList[0];

                for (int i = 1; i < 13; i++) {
                    res += ", "+windowCountList[i];
                }

                System.out.println(res);

            }
        });

        result.writeAsText(file_path, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();
    }


}
