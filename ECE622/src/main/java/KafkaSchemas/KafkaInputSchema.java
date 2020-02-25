package KafkaSchemas;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * Used to serialize-deserialize data so that can be processed by Kafka. This one is used for our custom modified initial stream
 */
public class KafkaInputSchema implements KafkaSerializationSchema<Tuple3<String,Double,String>>, KafkaDeserializationSchema<String> {

    private String topic;

    public KafkaInputSchema(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public boolean isEndOfStream(String s) {
        return false;
    }

    @Override
    public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        return new String(consumerRecord.value(), "UTF-8");
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple3<String, Double, String> element, @Nullable Long timestamp) {
        return new ProducerRecord<byte[], byte[]>(topic, (element.f0+ ";"+element.f1+";"+element.f2).getBytes(StandardCharsets.UTF_8));
    }
}
