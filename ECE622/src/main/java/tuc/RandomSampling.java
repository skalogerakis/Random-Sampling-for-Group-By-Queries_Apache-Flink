package tuc;

import jdk.nashorn.internal.objects.Global;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;

import javax.naming.Context;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import tuc.Calculations;


public class RandomSampling {

    //final static OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

    public static void main(String[] args) throws Exception {

        String inputTopic = "csvtokafka2";
        String outputTopic = "flink_output";
        String consumerGroup = "KafkaCsvProducer";
        String address = "localhost:9092";
        String pattern = "^\\bEndOfStream\\b$";

        String example = "location,city,country,utc,local,parameter,value,unit,latitude,longitude,attribution";
        String keys = "location,parameter";
        //List<Integer> integerList = null;


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

//        FlinkKafkaProducer producer = new FlinkKafkaProducer<String>(
//                outputTopic,
//                new KeyedSerializationSchemaWrapper<String>(new KafkaMsgSchema()),
//                p,
//                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

//        flinkKafkaConsumer.assignTimestampsAndWatermarks()
//        DataStream<String> stringInputStream = env
//                .addSource(flinkKafkaConsumer);
//
//        stringInputStream
//                .map(new WordsCapitalizer());

        //DataStream<Tuple2<String, Float>> csvInput = env.readTextFile("/home/skalogerakis/TUC_Projects/TUC_Advanced_Database_Systems/MyDocs/openaq_Bosnia.csv")

        //This is for data input purposes only. TODO replace that with Kafka implementation
        //Current way data are transformed Tuple2<String, Float>
        //DataStream<Tuple2<String, Double>> input = env.readTextFile("/home/skalogerakis/Downloads/openaq.csv")
        //TODO a tuple3 implementation in DummyClass
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

        DataStream<Tuple5<String,Double,Double,Double,Double>> sum = input
                .keyBy(0)
                .process(new CalcImplementation());
        sum.print();

        /**
         * New version with window
         * TODO check if we want to add timestamps and watermarks
         */
        //TODO MUST REMOVE LAST DUMMY ELEMENT
//        DataStream<Tuple6<String,Double,Double,Double,Double,Double>> sum = input
//                .keyBy(0)
//                .timeWindow(Time.seconds(30))
//                .process(new CalcImplemWindow())
//                ;
//        sum.print();
//
//        DataStream<Tuple2<String, Double>> finsum = sum
//                .flatMap(new FlatMapFunction<Tuple6<String,Double,Double,Double,Double,Double>, Tuple2<String, Double>>() {
//                    @Override
//                    public void flatMap(Tuple6<String,Double,Double,Double,Double,Double> value, Collector<Tuple2<String, Double>> out)
//                            throws Exception {
//                        //String[] words = value.split(",");
//                        Tuple2<String, Double> temp1 = new Tuple2<>("Total", value.f4);
//
//                        out.collect(temp1);
//                    }
//                })
//                .keyBy(0)
//                .timeWindow(Time.seconds(30))
//                .sum(1);
//
//
//        finsum.print();
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

//    public static FlinkKafkaProducer<String> createStringProducerForTopic(
//            String topic, String kafkaAddress){
//        Properties props = new Properties();
//        props.setProperty("bootstrap.servers", kafkaAddress);
//        //props.setProperty("group.id",kafkaGroup);
//
//        return new FlinkKafkaProducer<>(kafkaAddress,
//                topic, new SimpleStringSchema(),props);
//    }
    //TODO add checks when wrong input
    public static List<Integer> keyEval(String attributes, String keys){
        String[] attrSplitter = attributes.trim().split(",");
        String[] keySplitter = keys.trim().split(",");
        List<Integer> posList = new ArrayList<Integer>();

        for(int j=0; j<keySplitter.length;j++){
            for(int i=0; i<attrSplitter.length;i++){
                if(keySplitter[j].compareToIgnoreCase(attrSplitter[i])==0){
                    System.out.println("key "+ keySplitter[j]+ " from position" + i);
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


