package org.apache.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class ProvaCommenti {

    public static void main(String[] args) throws Exception {

        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(8);
        env.setRestartStrategy(RestartStrategies.noRestart());

        env.setStateBackend(new RocksDBStateBackend("file:///tmp"));

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "flink");

        DataStream<Comment> inputStream = env.addSource(new FlinkKafkaConsumer<>("flink", new CommentSchema(), properties))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Comment>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(Comment commentLog) {
                        return commentLog.createDate * 1000;
                    }
                });

        DataStream<Comment> not_filtered = inputStream.filter(x -> x.userID == 0);
        not_filtered.print().setParallelism(1);
        //not_filtered.map( x -> x.error).print().setParallelism(1);



        env.execute();
    }
}
