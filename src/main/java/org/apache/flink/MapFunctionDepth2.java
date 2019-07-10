package org.apache.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class MapFunctionDepth2 implements FlatMapFunction<Comment, Tuple3<String, String, String>>{

    private JedisPool jedisPool = null;

    public MapFunctionDepth2(JedisPool jedisPool){
        this.jedisPool = jedisPool;
    }

    public MapFunctionDepth2(){

    }

    @Override
    public void flatMap(Comment comment, Collector<Tuple3<String, String, String>> collector) throws Exception {

        try(Jedis jedis = JedisPoolFactory.getInstance().getResource()){
            String value = jedis.get(Integer.toString(comment.inReplyTo));

            if(value != null){

                //id commento padre, id user padre, id autore nonno
                Tuple3<String,String,String> t = new Tuple3<>(Integer.toString(comment.commentID), Integer.toString(comment.userID), value);

                collector.collect(t);
            }
        }catch(Exception e){
            System.err.println("Error depth 2: commento genitore non trovato!");
        }


    }
}
