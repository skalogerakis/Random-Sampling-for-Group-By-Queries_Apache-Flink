package KafkaSchemas;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * Used to serialize-deserialize data so that can be processed by Kafka. This one is used for our aggregation values to be transferred from job1 to job2
 */
public class KafkaTestSchema implements KafkaSerializationSchema<Tuple6<String,Double,Double,Double,Double,Double>>, KafkaDeserializationSchema<String> {

    private String topic;

    public KafkaTestSchema(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple6<String, Double, Double, Double, Double,Double> element, @Nullable Long timestamp) {
        return new ProducerRecord<byte[], byte[]>(topic, (element.f0+ ";"+element.f1+";"+element.f2+";"+element.f3+";"+element.f4+';'+element.f5 ).getBytes(StandardCharsets.UTF_8));

    }

    /**
     * Used at some point when trying to identify where stream ended. CURRENTLY NOT USED
     * @param s
     * @return
     */
    @Override
    public boolean isEndOfStream(String s) {
//        String regex = "^Total\\,(-)*(\\d.+){1}\\,-1\\.0{1}\\,-1\\.0{1}\\,-1\\.0{1}$";
//        if(s.matches(regex)){
//            System.out.println("Yaeh");
//            return true;
//        }
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
}
