package utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Preconditions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import tuc.Calculations;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class KafkaTestSchema implements KafkaSerializationSchema<Tuple6<String,Double,Double,Double,Double,Double>>, KafkaDeserializationSchema<String> {
    //TODO check for deserializer as well
    //https://github.com/zBrainiac/edge2ailab/blob/03cf62b2dfd38c35547e8f8d547ef514b21cddec/src/main/java/FlinkConsumer/iotConsumerFilter.java
    //https://www.fatalerrors.org/a/apache-flink-talk-series-14-1-kafka-of-data-stream-connectors.html?fbclid=IwAR0bUHM2GHR52mlotbuxcmKll-fpK2e33atYbzIJx2tQk8ZKXHWK3H2IK-Y
    private String topic;
    public boolean EOS=false;

    //private final DeserializationSchema<String> deserializationSchema;

    public KafkaTestSchema(String topic) {
        super();
        this.topic = topic;
        //this.EOS = false;
        //this.deserializationSchema = deserializationSchema;
    }
    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple6<String, Double, Double, Double, Double,Double> element, @Nullable Long timestamp) {
        return new ProducerRecord<byte[], byte[]>(topic, (element.f0+ ","+element.f1+","+element.f2+","+element.f3+","+element.f4+','+element.f5 ).getBytes(StandardCharsets.UTF_8));

    }


    //TODO REMOVE THAT
    @Override
    public boolean isEndOfStream(String s) {
        //System.out.println("EOS");
        String regex = "^Total\\,(-)*(\\d.+){1}\\,-1\\.0{1}\\,-1\\.0{1}\\,-1\\.0{1}$";
        //System.out.println(s);
        if(s.matches(regex)){
            System.out.println("Yaeh");
            this.EOS=true;
            return true;
        }


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
