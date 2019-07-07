package org.apache.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class MapFunctionDepth3 implements FlatMapFunction <CommentLog, Tuple2<String, Double>>{

    private JedisPool jedisPool = null;

    public MapFunctionDepth3(JedisPool jedisPool){
        this.jedisPool = jedisPool;
    }

    public MapFunctionDepth3(){

    }


    @Override
    public void flatMap(CommentLog commentLog, Collector<Tuple2<String, Double>> collector) throws Exception {
        try(Jedis jedis = JedisPoolFactory.getInstance().getResource()){

            String commentoPadre = Integer.toString(commentLog.inReplyTo);

            String[] values = jedis.get(commentoPadre).split("_");


            if(values[0] != null){
                Tuple2<String, Double> t1 = new Tuple2<>(values[0],1*0.7);
                collector.collect(t1);

            }
            if(values[1] != null){
                Tuple2<String, Double> t2 = new Tuple2<>(values[1],1*0.7);
                collector.collect(t2);

            }

        }catch(Exception e){
            System.err.println("Error depth 3");
        }
    }
}
