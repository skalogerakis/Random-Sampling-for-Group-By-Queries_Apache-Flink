package tuc;

import jdk.nashorn.internal.objects.Global;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import javax.naming.Context;
import java.lang.reflect.Array;
import java.nio.charset.Charset;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.producer.ProducerRecord;
import tuc.Calculations;
//import utils.JoinProcessFunction;
import utils.KafkaInputSchema;
import utils.KafkaMsgSchema;
import utils.KafkaTestSchema;


public class RandomSampling {

    //final static OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

    public static void main(String[] args) throws Exception {

        String inputTopic = "csvkafka";

        String inputNewTopic = "flinkout1";
        String outputTopic = "flinkaggr1";

        String consumerGroup = "KafkaCsvProducer";
        String address = "localhost:9092";
        String pattern = "^\\bEndOfStream\\b$";

        String example = "location,city,country,utc,local,parameter,value,unit,latitude,longitude,attribution";
        String keys = "location";
        String aggr = "value";

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        //TODO check what is going on with time
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //List<Integer> integerList = keyEval(example,keys);
        HashMap<String,List<Integer>> integerList = attrEval(example,keys,aggr);

        /**
         * IMPORTANT: Messages sent by a kafka producer to a particular topic partition will be appended in the order they are sent.
         */
        FlinkKafkaConsumer<String> flinkKafkaConsumer = createStringConsumerForTopic(
                inputTopic, address, consumerGroup);
        //TODO enable that
        flinkKafkaConsumer.setStartFromEarliest();

        FlinkKafkaProducer<Tuple6<String,Double,Double,Double,Double,Double>> flinkKafkaProducer = createStringProducer2(
                outputTopic, address);
        flinkKafkaProducer.setWriteTimestampToKafka(true);

        FlinkKafkaProducer<Tuple3<String,Double,String>> flinkKafkaProducerInput = createStringProducerInput(
                inputNewTopic, address);
        flinkKafkaProducerInput.setWriteTimestampToKafka(true);

        List<Integer> keyPosList = integerList.get("key");
        List<Integer> attrPosList = integerList.get("attr");
        List<Integer> aggrPosList = integerList.get("aggr");

        DataStream<Tuple3<String, Double,String>> input = env.addSource(flinkKafkaConsumer)
                .flatMap(new FlatMapFunction<String, Tuple3<String,Double,String>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple3<String, Double,String>> out)
                            throws Exception {


                        List<String> tempkey = new ArrayList<String>();
                        List<String> tempattr = new ArrayList<String>();
                        //List<String> tempaggr = new ArrayList<String>();
                        //TODO works only for one element right now
                        int tempaggr = aggrPosList.get(0);

                        String[] words = value.split(",");

                        for(int i = 0; i< keyPosList.size(); i++){
                            tempkey.add(words[keyPosList.get(i)]);
                        }
                        String finalKey = String.join(",",tempkey);

                        for(int i = 0; i< attrPosList.size(); i++){
                            tempattr.add(words[attrPosList.get(i)]);
                        }
                        String finalAttr = String.join(",",tempattr);

                        Double finalAggr = Double.parseDouble(words[tempaggr]);

                        Tuple3<String,Double,String> temp1;

                        temp1 = new Tuple3<>(finalKey,finalAggr,finalAttr);
                        tempkey.clear();
                        tempattr.clear();
                        out.collect(temp1);

                    }
                });

        //input.print();

        //TODO add sink and write input in another topic formatted in specific way
        input.addSink(flinkKafkaProducerInput);


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

        DataStream<Tuple6<String,Double,Double,Double,Double,String>> sum = input
                .keyBy(0)
                .timeWindow(Time.seconds(30))
                .process(new CalcImplemWindow())
                ;
        sum.print();

        DataStream<Tuple5<String,Double,Double,Double,Double>> finsum = sum
                .flatMap(new FlatMapFunction<Tuple6<String,Double,Double,Double,Double,String>, Tuple5<String,Double,Double,Double,Double>>() {
                    @Override
                    public void flatMap(Tuple6<String,Double,Double,Double,Double,String> value, Collector<Tuple5<String,Double,Double,Double,Double>> out)
                            throws Exception {
                        //String[] words = value.split(",");
                        Tuple5<String,Double,Double,Double,Double> temp1 = new Tuple5<>("Total", value.f4,-1D,-1D,-1D);

                        //System.out.println("THIS is "+temp1.toString());
                        out.collect(temp1);
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(30))
                .sum(1)
                ;


        finsum.print();

        DataStream<Tuple6<String,Double,Double,Double,Double,Double>> joinedStream= sum
                .join(finsum)
                .where(new KeySelector<Tuple6<String,Double,Double,Double,Double,String>, String>() {
                    @Override
                    public String getKey(Tuple6<String,Double,Double,Double,Double,String> stringStringTuple6) throws Exception {
                        return stringStringTuple6.f5;
                    }
                })
                .equalTo(new KeySelector<Tuple5<String,Double,Double,Double,Double>, String>() {
                    @Override
                    public String getKey(Tuple5<String,Double,Double,Double,Double> stringDoubleDoubleDoubleTuple5) throws Exception {
                        return stringDoubleDoubleDoubleTuple5.f0;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                //.allowedLateness(Time.seconds(10))
                .apply(new JoinFunction<Tuple6<String,Double,Double,Double,Double,String>, Tuple5<String,Double,Double,Double,Double>, Tuple6<String, Double, Double, Double, Double,Double>>() {
                    @Override
                    public Tuple6<String, Double, Double, Double, Double,Double> join(Tuple6<String,Double,Double,Double,Double,String> join1, Tuple5<String,Double,Double,Double,Double> join2) throws Exception {
                        return new Tuple6<>(join1.f0,join1.f1,join1.f2,join1.f3,join1.f4,join2.f1);
                    }
                })
                ;
        //finsum.union(sum).print();

        joinedStream.print();
        joinedStream.addSink(flinkKafkaProducer);

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

    public static FlinkKafkaProducer<Tuple6<String,Double,Double,Double,Double,Double>> createStringProducer2(
            String topic, String kafkaAddress){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);
        FlinkKafkaProducer<Tuple6<String,Double,Double,Double,Double,Double>> producer =  new FlinkKafkaProducer<Tuple6<String,Double,Double,Double,Double,Double>>(topic, new KafkaTestSchema(topic),props,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        return producer;
    }

    public static FlinkKafkaProducer<Tuple3<String,Double,String>> createStringProducerInput(
            String topic, String kafkaAddress){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);
        //TODO implemented based on this
        //https://github.com/luweizheng/flink-tutorials/blob/2a72c375d182cc47da016627023083ba85808f96/src/main/java/com/flink/tutorials/java/projects/wordcount/WordCountKafkaInKafkaOut.java
        //FlinkKafkaProducer producer = new FlinkKafkaProducer<>(topic, (SerializationSchema<Tuple5<String,Double,Double,Double,Double>>) new KafkaTestSchema(),props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        FlinkKafkaProducer<Tuple3<String,Double,String>> producer =  new FlinkKafkaProducer<Tuple3<String,Double,String>>(topic, new KafkaInputSchema(topic),props,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        return producer;
    }



    //TODO add checks when wrong input
    //TODO CHECK IF WE CAN HAVE MULTIPLE AGGREGATION. FOR NOW CAN BE USED ONLY WITH ONE
    public static HashMap<String,List<Integer>> attrEval(String attributes, String keys, String aggregation){
        String[] attrSplitter = attributes.trim().split(",");
        String[] keySplitter = keys.trim().split(",");
        //List<Integer> posList = new ArrayList<Integer>();

        HashMap<String,List<Integer>> attrParser = new HashMap<String,List<Integer>>();
        List<Integer> keyPosList = new ArrayList<Integer>();
        List<Integer> attrPosList = new ArrayList<Integer>();
        List<Integer> aggrPosList = new ArrayList<Integer>();


        for(int j=0; j<keySplitter.length;j++){
            for(int i=0; i<attrSplitter.length;i++){
                if(keySplitter[j].compareToIgnoreCase(attrSplitter[i])==0){
                    keyPosList.add(i);
                    //System.out.println("key "+i);
                }else{
                    attrPosList.add(i);
                    //System.out.println("attr "+i);
                    //System.out.println(attrSplitter[j] +" "+aggregation);

                }
                if(attrSplitter[i].compareToIgnoreCase(aggregation)==0){
                    aggrPosList.add(i);
                    //System.out.println("aggr "+i);
                }
            }
        }
        attrParser.put("key",keyPosList);
        attrParser.put("attr",attrPosList);
        attrParser.put("aggr",aggrPosList);

        return attrParser;
    }



}


