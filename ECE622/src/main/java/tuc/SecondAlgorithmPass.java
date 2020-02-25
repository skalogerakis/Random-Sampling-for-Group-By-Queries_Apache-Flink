package tuc;

import KafkaSchemas.KafkaFinalSchema;
import KafkaSchemas.KafkaInputSchema;
import KafkaSchemas.KafkaTestSchema;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class SecondAlgorithmPass {

    /**
     * TODO that description
     * NAME:SecongAlgorithmPass
     * Description:
     * @param args [optional] -p(parallellism){default value 1} -input-topic {default init stream value input-topic-job1} -output-topic {default value output-topic-job2}
     *             -aggr-topic {default init stream value output-topic-job1} -consumer-group {default value KafkaCsvProducer} -ip {default value localhost:9092} -windows-time {default value 60}
     *             -M{default value 20}
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        int parallel = 1;
        ParameterTool parameterTool=null;

        try{
            parameterTool = ParameterTool.fromArgs(args);
        }catch(IllegalArgumentException io){
            System.out.println("Error while parsing arguments. Please prefix keys with -- or -.");
            System.out.println("[optional] -p(parallellism){default value 1} -input-topic {default init stream value input-topic-job1} -output-topic {default value output-topic-job2}\n" +
                    "                   -aggr-topic {default init stream value output-topic-job1} -consumer-group {default value KafkaCsvProducer} -ip {default value localhost:9092} -windows-time {default value 60}\n" +
                    "                   -M{default value 20}");
            System.exit(-1);
        }


        String inputNewTopic = parameterTool.get("input-topic","input-topic-job1");
        String inputTopic = "_"+inputNewTopic;
        String inputAggr = parameterTool.get("aggr-topic","output-topic-job1");
        String outputTopic = parameterTool.get("output-topic","output-topic-job2");
        String consumerGroup = parameterTool.get("consumer-group","KafkaCsvProducer");

        parallel = parameterTool.getInt("p",1);

        String address = parameterTool.get("ip","localhost:9092");
        int windowTime = parameterTool.getInt("windows-time",60);
        double M = parameterTool.getDouble("M",20.0D);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //parallelism definition
        env.setParallelism(parallel);

        //Used Ingestion time as time characteristic
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        //---------------------------------------------------------------------------
        //                             KAFKA CONSUMERS
        //---------------------------------------------------------------------------
        /**
         * IMPORTANT: Messages sent by a kafka producer to a particular topic partition will be appended in the order they are sent.
         */
        FlinkKafkaConsumer<String> flinkKafkaConsumer = createStringConsumerForAggr(
                inputAggr, address, consumerGroup);
        flinkKafkaConsumer.setStartFromEarliest();

        FlinkKafkaConsumer<String> flinkKafkaConsumerInput = createStringConsumerForInput(
                inputTopic, address, consumerGroup);
        flinkKafkaConsumerInput.setStartFromEarliest();

        //---------------------------------------------------------------------------
        //                             KAFKA PRODUCERS
        //---------------------------------------------------------------------------
        FlinkKafkaProducer<Tuple2<String,String>> flinkKafkaProducer = createStringProducer(
                outputTopic, address);
        flinkKafkaProducer.setWriteTimestampToKafka(true);


        DataStream<Tuple4<String,Double,Double,Double>> inputAg = env
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

        DataStream<Tuple2<String,String>> inputStream = env.addSource(flinkKafkaConsumerInput)
                .flatMap(new FlatMapFunction<String, Tuple2<String,String>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String,String>> out)
                            throws Exception {

                        String[] words = value.split(";");
                        //Also available in words[1] aggregate

                        Tuple2<String,String> temp1 = new Tuple2<>(words[0],words[2]);
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
                .window(TumblingEventTimeWindows.of(Time.seconds(windowTime)))
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

        //Print execution plan for visualisation purposes
        //https://flink.apache.org/visualizer/
        System.out.println(env.getExecutionPlan());
        env.execute("SecondAlgorithmPass");

    }

    /**
     * First consumer that reads data from aggregation topic(Completed in FirstAlgorithm pass)
     * @param topic
     * @param kafkaAddress
     * @param kafkaGroup
     * @return
     */
    public static FlinkKafkaConsumer<String> createStringConsumerForAggr(
            String topic, String kafkaAddress, String kafkaGroup ) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);
        props.setProperty("group.id",kafkaGroup);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(topic, new KafkaTestSchema(topic), props);

        return consumer;
    }

    /**
     * Second consumer that reads out modified initial stream(Completed in FirstAlgorithm pass)
     * @param topic
     * @param kafkaAddress
     * @param kafkaGroup
     * @return
     */
    public static FlinkKafkaConsumer<String> createStringConsumerForInput(
            String topic, String kafkaAddress, String kafkaGroup ) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);
        props.setProperty("group.id",kafkaGroup);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(topic, new KafkaInputSchema(topic), props);
        return consumer;
    }

    /**
     * Producer that writes our output result to a final output topic
     * @param topic
     * @param kafkaAddress
     * @return
     */
    public static FlinkKafkaProducer<Tuple2<String,String>> createStringProducer(
            String topic, String kafkaAddress){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);
        FlinkKafkaProducer<Tuple2<String,String>> producer =  new FlinkKafkaProducer<Tuple2<String,String>>(topic, new KafkaFinalSchema(topic),props,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        return producer;
    }
}