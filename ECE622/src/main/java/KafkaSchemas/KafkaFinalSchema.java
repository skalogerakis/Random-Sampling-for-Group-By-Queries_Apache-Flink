package KafkaSchemas;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * Used to serialize data so that can be processed by Kafka. This one is used for our final output stream
 */
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
