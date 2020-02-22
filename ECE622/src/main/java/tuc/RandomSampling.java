package tuc;

import jdk.nashorn.internal.objects.Global;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import javax.naming.Context;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.producer.ProducerRecord;
import tuc.Calculations;
import utils.KafkaMsgSchema;
import utils.KafkaTestSchema;


public class RandomSampling {

    //final static OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

    public static void main(String[] args) throws Exception {

        String inputTopic = "csvtokafka2";
        String outputTopic = "flink2";
        String consumerGroup = "KafkaCsvProducer";
        String address = "localhost:9092";
        String pattern = "^\\bEndOfStream\\b$";

        String example = "location,city,country,utc,local,parameter,value,unit,latitude,longitude,attribution";
        String keys = "location";
        //List<Integer> integerList = null;

//        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
//
//        if (parameterTool.getNumberOfParameters() < 3) {
//            System.out.println("Missing parameters!\n" +
//                    "Usage: --numRecords <numRecords> --index <index> --type <type>");
//            return;
//        }



        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        //TODO check what is going on with time
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Pattern r = Pattern.compile(pattern);

        List<Integer> integerList = keyEval(example,keys);

        /**
         * IMPORTANT: Messages sent by a kafka producer to a particular topic partition will be appended in the order they are sent.
         */
        FlinkKafkaConsumer<String> flinkKafkaConsumer = createStringConsumerForTopic(
                inputTopic, address, consumerGroup);
        //TODO enable that
        flinkKafkaConsumer.setStartFromEarliest();

        FlinkKafkaProducer<Tuple5<String,Double,Double,Double,Double>> flinkKafkaProducer = createStringProducer2(
                outputTopic, address);

        flinkKafkaProducer.setWriteTimestampToKafka(true);

        //List<Integer> finalIntegerList = integerList;
        DataStream<Tuple2<String, Double>> input = env.addSource(flinkKafkaConsumer)
                .flatMap(new FlatMapFunction<String, Tuple2<String,Double>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Double>> out)
                            throws Exception {
                        String[] words = value.split(",");
                        //Matcher m = r.matcher(words[0]);
                        Tuple2<String,Double> temp1;
                        //System.out.println("ERROREOER " + words[0]);
                        //if(m.matches()){
                        List<String> tempString = new ArrayList<String>();;
                        for(int i = 0; i< integerList.size(); i++){
                            tempString.add(words[integerList.get(i)]);
                        }
                        String finaltem = String.join(",",tempString);

                           // temp1 = new Tuple2<>(words[0]+","+words[5],0.0D);
                        //}else{
                            temp1 = new Tuple2<>(finaltem,Double.parseDouble(words[6]));

                        //}
                        //System.out.println(temp1);
                        out.collect(temp1);
                    }
                });

        /**
         *          key   sum   count   mean   var
         * TODO INIT version without window
         */

//        DataStream<Tuple5<String,Double,Double,Double,Double>> sum = input
//                .keyBy(0)
//                .process(new CalcImplementation());
//        sum.print();
//        sum.addSink(flinkKafkaProducer);


        /**
         * New version with window
         * TODO check if we want to add timestamps and watermarks
         */
        //TODO MUST REMOVE LAST DUMMY ELEMENT
        DataStream<Tuple5<String,Double,Double,Double,Double>> sum = input
                .keyBy(0)
                .timeWindow(Time.seconds(30))
                .process(new CalcImplemWindow())
                ;
        sum.print();

        sum.addSink(flinkKafkaProducer);

        DataStream<Tuple5<String,Double,Double,Double,Double>> finsum = sum
                .flatMap(new FlatMapFunction<Tuple5<String,Double,Double,Double,Double>, Tuple5<String,Double,Double,Double,Double>>() {
                    @Override
                    public void flatMap(Tuple5<String,Double,Double,Double,Double> value, Collector<Tuple5<String,Double,Double,Double,Double>> out)
                            throws Exception {
                        //String[] words = value.split(",");
                        Tuple5<String,Double,Double,Double,Double> temp1 = new Tuple5<>("Total", value.f4,-1D,-1D,-1D);
                        out.collect(temp1);
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(30))
                .sum(1)
                ;

        finsum.print();
        finsum.addSink(flinkKafkaProducer);

        //execute program to see action
        env.execute("Streaming for Random Sampling");

    }





    public static FlinkKafkaConsumer<String> createStringConsumerForTopic(
            String topic, String kafkaAddress, String kafkaGroup ) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);
        props.setProperty("group.id",kafkaGroup);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                topic, new SimpleStringSchema(), props);

        return consumer;
    }

    /**
     * This works but is deprecated.TODO find a better way to do that
     * @param topic
     * @param kafkaAddress
     * @return
     */
    @Deprecated
    public static FlinkKafkaProducer<Tuple5<String,Double,Double,Double,Double>> createStringProducer(
            String topic, String kafkaAddress){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);

        FlinkKafkaProducer producer = new FlinkKafkaProducer<>(kafkaAddress, topic, (SerializationSchema<Tuple5<String,Double,Double,Double,Double>>) new KafkaMsgSchema());

        return producer;
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

    //TODO add checks when wrong input
    public static List<Integer> keyEval(String attributes, String keys){
        String[] attrSplitter = attributes.trim().split(",");
        String[] keySplitter = keys.trim().split(",");
        List<Integer> posList = new ArrayList<Integer>();

        for(int j=0; j<keySplitter.length;j++){
            for(int i=0; i<attrSplitter.length;i++){
                if(keySplitter[j].compareToIgnoreCase(attrSplitter[i])==0){
                    //System.out.println("key "+ keySplitter[j]+ " from position" + i);
                    posList.add(i);
                }
            }
        }
        //TODO return list of position
        //TODO maybe insert everything in a list and then concat
        //https://mkyong.com/java/java-how-to-join-list-string-with-commas/
        return posList;
    }



}


