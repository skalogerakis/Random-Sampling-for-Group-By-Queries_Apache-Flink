package tuc;

import KafkaSchemas.KafkaFinalSchema;
import KafkaSchemas.KafkaInputSchema;
import KafkaSchemas.KafkaTestSchema;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class Job2 {


    public static void main(String[] args) throws Exception {

        String inputTopic = "flinkout1";
        String inputAggr = "flinkaggr1";
        String outputTopic = "flinkfinal";
        String consumerGroup = "KafkaCsvProducer";
        String address = "localhost:9092";

        double M =100.0D;
        // set up the execution environment
        final StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();

        env2.setParallelism(4);


        env2.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        FlinkKafkaProducer<Tuple2<String,String>> flinkKafkaProducer = createStringProducer2(
                outputTopic, address);

        flinkKafkaProducer.setWriteTimestampToKafka(true);

        /**
         * IMPORTANT: Messages sent by a kafka producer to a particular topic partition will be appended in the order they are sent.
         */
        FlinkKafkaConsumer<String> flinkKafkaConsumer = createStringConsumerForTopic(
                inputAggr, address, consumerGroup);
        //TODO disable that in this consumer enable in the other one
        flinkKafkaConsumer.setStartFromEarliest();


        DataStream<Tuple4<String,Double,Double,Double>> inputAg = env2
                .addSource(flinkKafkaConsumer)
                .flatMap(new FlatMapFunction<String, Tuple4<String,Double,Double,Double>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple4<String,Double,Double,Double>> out)
                            throws Exception {
                        String[] words = value.split(",");
                        double si = Math.round(M * (Double.parseDouble(words[4]) / Double.parseDouble(words[5])));//M*γi/γ
                        out.collect(new Tuple4<>(words[0], Double.parseDouble(words[3]), Double.parseDouble(words[5]), si));
                    }});


        //inputAg.print();

        FlinkKafkaConsumer<String> flinkKafkaConsumerInput = createStringConsumerForInput(
                inputTopic, address, consumerGroup);
        //TODO disable that in this consumer enable in the other one
        flinkKafkaConsumerInput.setStartFromEarliest();

        //TODO do not print second aggregate attribute
        DataStream<Tuple2<String,String>> inputStream = env2.addSource(flinkKafkaConsumerInput)
                .flatMap(new FlatMapFunction<String, Tuple2<String,String>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String,String>> out)
                            throws Exception {

                        String[] words = value.split(";");
                        //Also available in words[1] aggregate
                        Tuple2<String,String> temp1;

                        temp1 = new Tuple2<>(words[0],words[2]);

                        out.collect(temp1);

                    }
                });

        //inputStream.print();

        // key  args   γi     γ      si
        DataStream<Tuple5<String,String,Double,Double,Double>> joinedStream= inputStream
                .join(inputAg)
                .where(new KeySelector<Tuple2<String, String>, String>() {
                    @Override
                    public String getKey(Tuple2<String, String> stringStringTuple2) throws Exception {
                        return stringStringTuple2.f0;
                    }
                })
                .equalTo(new KeySelector<Tuple4<String, Double, Double, Double>, String>() {
                    @Override
                    public String getKey(Tuple4<String, Double, Double, Double> stringDoubleDoubleDoubleTuple4) throws Exception {
                        return stringDoubleDoubleDoubleTuple4.f0;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(50)))
                //.allowedLateness(Time.seconds(10))
                .apply(new JoinFunction<Tuple2<String, String>, Tuple4<String, Double, Double, Double>, Tuple5<String, String, Double, Double, Double>>() {
                    @Override
                    public Tuple5<String, String, Double, Double, Double> join(Tuple2<String, String> stringStringTuple2, Tuple4<String, Double, Double, Double> stringDoubleDoubleDoubleTuple4) throws Exception {
                        return new Tuple5<>(stringStringTuple2.f0,stringStringTuple2.f1,stringDoubleDoubleDoubleTuple4.f1,stringDoubleDoubleDoubleTuple4.f2,stringDoubleDoubleDoubleTuple4.f3);
                    }
                })
                ;


        DataStream<Tuple2<String,String>> sample =joinedStream
                .keyBy(0)
                .process(new ReservoirSampler())
                ;


        sample.keyBy(0).print();

        sample.keyBy(0).addSink(flinkKafkaProducer);
        env2.execute("Job2");

    }

    public static FlinkKafkaConsumer<String> createStringConsumerForTopic(
            String topic, String kafkaAddress, String kafkaGroup ) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);
        props.setProperty("group.id",kafkaGroup);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(topic, new KafkaTestSchema(topic), props);

        return consumer;
    }


    public static FlinkKafkaConsumer<String> createStringConsumerForInput(
            String topic, String kafkaAddress, String kafkaGroup ) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);
        props.setProperty("group.id",kafkaGroup);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(topic, new KafkaInputSchema(topic), props);

        return consumer;
    }


    public static FlinkKafkaProducer<Tuple2<String,String>> createStringProducer2(
            String topic, String kafkaAddress){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);
        FlinkKafkaProducer<Tuple2<String,String>> producer =  new FlinkKafkaProducer<Tuple2<String,String>>(topic, new KafkaFinalSchema(topic),props,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        return producer;
    }
}