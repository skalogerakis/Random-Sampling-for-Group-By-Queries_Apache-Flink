package utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class KafkaInputSchema implements KafkaSerializationSchema<Tuple3<String,Double,String>>, KafkaDeserializationSchema<String> {

    private String topic;

    public KafkaInputSchema(String topic) {
        super();
        this.topic = topic;
        //this.EOS = false;
        //this.deserializationSchema = deserializationSchema;
    }

    @Override
    public boolean isEndOfStream(String s) {
        return false;
    }

    @Override
    public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        return null;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return null;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple3<String, Double, String> element, @Nullable Long timestamp) {
        return null;
    }
}
