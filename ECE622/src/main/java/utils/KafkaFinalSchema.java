package utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class KafkaFinalSchema implements KafkaSerializationSchema<Tuple2<String,String>> {

    private String topic;

    public KafkaFinalSchema(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, String> element, @Nullable Long aLong) {
        return new ProducerRecord<byte[], byte[]>(topic, (element.f0+ ","+element.f1).getBytes(StandardCharsets.UTF_8));
    }
}
