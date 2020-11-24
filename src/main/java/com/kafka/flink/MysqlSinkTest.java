package com.kafka.flink;

import com.kafka.flink.MysqlSink;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import scala.Tuple3;

import java.util.Properties;

public class MysqlSinkTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "prod-node4:9092");

// 1,abc,100  类似这样的数据，当然也可以是很复杂的json数据，去做解析
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("sqlserver_xj.dbo.Emp_GpsDtata", new SimpleStringSchema(), properties);
        env.getConfig().disableSysoutLogging();  //设置此可以屏蔽掉日记打印情况
        env.getConfig().setRestartStrategy(
                RestartStrategies.fixedDelayRestart(5, 5000));
        env.enableCheckpointing(2000);
        DataStream<String> stream = env.addSource(consumer);

        DataStream<Tuple3<String, String, String>> sourceStream = stream.filter((FilterFunction<String>) value -> StringUtils.isNotBlank(value))
                .map((MapFunction<String, Tuple3<String, String, String>>) value -> {
                    String[] args1 = value.split(",");
                    return new Tuple3<String, String, String>(
                            (args1[0]), args1[1],(args1[2]));
                });

        sourceStream.addSink(new MysqlSink());
        env.execute("data to mysql start");
    }
}

