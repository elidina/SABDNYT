package org.apache.flink.test;

import org.apache.flink.entities.Comment;
import org.apache.flink.utils.JedisPoolFactory;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class MapFunctionDepthTest2 implements FlatMapFunction<Comment, Tuple4<String, String, String, Long>>{

    private JedisPool jedisPool = null;

    public MapFunctionDepthTest2(JedisPool jedisPool){
        this.jedisPool = jedisPool;
    }

    public MapFunctionDepthTest2(){

    }

    @Override
    public void flatMap(Comment comment, Collector<Tuple4<String, String, String, Long>> collector) throws Exception {

        try(Jedis jedis = JedisPoolFactory.getInstance().getResource()){

            String value = jedis.get(Integer.toString(comment.inReplyTo));

            if(value != null){

                //id commento padre, id user padre, id autore nonno
                Tuple4<String,String,String, Long> t = new Tuple4<>(Integer.toString(comment.commentID), Integer.toString(comment.userID), value,comment.arrivalTime);

                collector.collect(t);
            }
        }catch(Exception e){
            System.err.println("Error depth 2: commento genitore non trovato!");
        }


    }
}
