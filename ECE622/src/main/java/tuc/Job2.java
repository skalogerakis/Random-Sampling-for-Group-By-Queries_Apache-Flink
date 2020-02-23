package tuc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import sun.awt.X11.XSystemTrayPeer;
import utils.KafkaMsgSchema;
import utils.KafkaTestSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

public class Job2 {

    /**
     * TODO on Job2
     * -Support version with dynamic keys
     * @param args
     * @throws Exception
     */

    public static void main(String[] args) throws Exception {

        String inputTopic = "flink2";
        //String outputTopic = "flink_out";
        String consumerGroup = "KafkaCsvProducer";
        String address = "localhost:9092";


        // set up the execution environment
        final StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();

        env2.setParallelism(4);


        env2.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);



        /**
         * IMPORTANT: Messages sent by a kafka producer to a particular topic partition will be appended in the order they are sent.
         */
        FlinkKafkaConsumer<String> flinkKafkaConsumer = createStringConsumerForTopic(
                inputTopic, address, consumerGroup);
        //TODO disable that in this consumer enable in the other one
        flinkKafkaConsumer.setStartFromEarliest();

       // flinkKafkaConsumer.notifyCheckpointComplete();

//        KafkaTestSchema test = new KafkaTestSchema(inputTopic);
//
//        if(test.isEndOfStream(flinkKafkaConsumer.toString())){
//            System.out.println("This is it.");
//        }


        DataStream<Tuple5<String,Double,Double,Double,Double>> input = env2.addSource(flinkKafkaConsumer)
                .flatMap(new FlatMapFunction<String, Tuple5<String,Double,Double,Double,Double>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple5<String,Double,Double,Double,Double>> out)
                            throws Exception {
                        String[] words = value.trim().split(",");

                        Tuple5<String,Double,Double,Double,Double> temp1;
//                        for(int i=0;i<5;i++){
//                            System.out.println(words[i]);
//                        }

                        temp1 = new Tuple5<>(words[0],Double.parseDouble(words[1]),Double.parseDouble(words[2]),Double.parseDouble(words[3]),Double.parseDouble(words[4]));

                        //}
                        //System.out.println(temp1);
                        out.collect(temp1);
                    }
                });
        input.print();

//        FlinkKafkaProducer<Tuple5<String,Double,Double,Double,Double>> flinkKafkaProducer = createStringProducer2(
//                outputTopic, address);
//
//        flinkKafkaProducer.setWriteTimestampToKafka(true);

        env2.execute("Job2");

    }





    public static FlinkKafkaConsumer<String> createStringConsumerForTopic(
            String topic, String kafkaAddress, String kafkaGroup ) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);
        props.setProperty("group.id",kafkaGroup);
        KafkaTestSchema t = new KafkaTestSchema(topic);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(
                topic, t, props);
        //System.out.println("THIS IS IT "+t.EOS);
        return consumer;
    }

    public static FlinkKafkaProducer<Tuple5<String,Double,Double,Double,Double>> createStringProducer2(
            String topic, String kafkaAddress){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);
        //TODO implemented based on this
        //https://github.com/luweizheng/flink-tutorials/blob/2a72c375d182cc47da016627023083ba85808f96/src/main/java/com/flink/tutorials/java/projects/wordcount/WordCountKafkaInKafkaOut.java
        //FlinkKafkaProducer producer = new FlinkKafkaProducer<>(topic, (SerializationSchema<Tuple5<String,Double,Double,Double,Double>>) new KafkaTestSchema(),props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        FlinkKafkaProducer<Tuple5<String,Double,Double,Double,Double>> producer =  new FlinkKafkaProducer<Tuple5<String,Double,Double,Double,Double>>(topic, new KafkaTestSchema(topic),props,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        return producer;
    }
}
