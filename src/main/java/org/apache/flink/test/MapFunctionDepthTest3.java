package org.apache.flink.test;

import org.apache.flink.entities.Comment;
import org.apache.flink.utils.JedisPoolFactory;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class MapFunctionDepthTest3 implements FlatMapFunction <Comment, Tuple3<String, Double, Long>>{

    private JedisPool jedisPool = null;

    public MapFunctionDepthTest3(JedisPool jedisPool){
        this.jedisPool = jedisPool;
    }

    public MapFunctionDepthTest3(){

    }

    @Override
    public void flatMap(Comment comment, Collector<Tuple3<String, Double, Long>> collector) throws Exception {
        try(Jedis jedis = JedisPoolFactory.getInstance().getResource()){

            String commentoPadre = Integer.toString(comment.inReplyTo);

            String[] values = jedis.get(commentoPadre).split("_");

            if(values[0] != null){
                Tuple3<String, Double, Long> t1 = new Tuple3<>(values[0],1*0.7, comment.arrivalTime);
                collector.collect(t1);
            }

            if(values[1] != null){
                Tuple3<String, Double, Long> t2 = new Tuple3<>(values[1],1*0.7, comment.arrivalTime);
                collector.collect(t2);
            }

        }catch(Exception e){
            System.err.println("Error depth 3: commento genitore non trovato! CommentID: "+ comment.commentID);
        }
    }
}

