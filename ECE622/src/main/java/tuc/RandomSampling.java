package tuc;

import jdk.nashorn.internal.objects.Global;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
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
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import tuc.Calculations;


public class RandomSampling {

    //final static OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

    public static void main(String[] args) throws Exception {

        String inputTopic = "csvtokafka";
        String outputTopic = "flink_output";
        String consumerGroup = "KafkaCsvProducer";
        String address = "localhost:9092";
        String pattern = "^\\bEndOfStream\\b$";

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO check what is going on with time
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Pattern r = Pattern.compile(pattern);

        /**
         * IMPORTANT: Messages sent by a kafka producer to a particular topic partition will be appended in the order they are sent.
         */
        FlinkKafkaConsumer<String> flinkKafkaConsumer = createStringConsumerForTopic(
                inputTopic, address, consumerGroup);
        //TODO enable that
        //flinkKafkaConsumer.setStartFromEarliest();

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
        DataStream<Tuple2<String, Double>> input = env.addSource(flinkKafkaConsumer)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Double>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Double>> out)
                            throws Exception {
                        String[] words = value.split(",");
                        Matcher m = r.matcher(words[0]);
                        Tuple2<String, Double> temp1;
                        //System.out.println("ERROREOER " + words[0]);
                        if(m.matches()){

                            temp1 = new Tuple2<>(words[0], 0.0D);
                        }else{
                            temp1 = new Tuple2<>(words[0], Double.parseDouble(words[6]));

                        }
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

        /**
         * New version with window
         * TODO check if we want to add timestamps and watermarks
         */
        DataStream<Tuple6<String,Double,Double,Double,Double,Double>> sum = input
                .keyBy(0)
                .timeWindow(Time.seconds(30))
                .process(new CalcImplemWindow())

                ;
        sum.print();

        DataStream<Tuple6<String,Double,Double,Double,Double,Double>> finsum = sum
                .keyBy(0)
                .sum(5)
                ;

        finsum.print();
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




}


