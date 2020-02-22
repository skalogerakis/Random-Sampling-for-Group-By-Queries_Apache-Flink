package utils;


import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;

public class KafkaMsgSchema implements SerializationSchema<Tuple5<String,Double,Double,Double,Double>> {

    private static final long serialVersionUID = 1L;

    public TypeInformation<String> getProducedType() {
        // Define the generated data Typeinfo
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

    public String getTargetTopic(Tuple5<String,Double,Double,Double,Double> element) {
        // use always the default topic
        return null;
    }

    @Override
    public byte[] serialize(Tuple5<String,Double,Double,Double,Double> element) {

        return (element.getField(0).toString()+ ","+element.getField(1).toString()+","+element.getField(2).toString()+","+element.getField(3).toString()+","+element.getField(4).toString()).getBytes();
    }

}