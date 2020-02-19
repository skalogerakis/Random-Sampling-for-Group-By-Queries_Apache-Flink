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
import org.apache.flink.api.common.time.Time;
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

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class RandomSampling {

    //final static OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

    public static void main(String[] args) throws Exception {

        String inputTopic = "flinkInput2";
        String outputTopic = "flink_output";
        String consumerGroup = "KafkaCsvProducer";
        String address = "localhost:9092";
        String pattern = "^\\bEndOfStream\\b$";

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        Pattern r = Pattern.compile(pattern);

        FlinkKafkaConsumer<String> flinkKafkaConsumer = createStringConsumerForTopic(
                inputTopic, address, consumerGroup);

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
                        System.out.println(temp1);
                        out.collect(temp1);
                    }
                });

        //FlatMap implementation
        //DataStream<Tuple3<String, Float,Float>> sum = input.keyBy(0).flatMap(new CalcImplementation());

        //KeyedProcessFunction implementation
        //                  key   sum   count   mean   var
        DataStream<Tuple5<String,Double,Double,Double,Double>> sum = input
                .keyBy(0)
                .process(new CalcImplementation());
                //.assignTimestampsAndWatermarks(new MyTimestampsAndWatermarks());

        sum.print();
        //TOOD try this https://training.ververica.com/exercises/carSegments.html

//        DataStream<Tuple5<String,Double,Double,Double,Double>> fin = sum
//                .keyBy(0)
//                .window(Time.seconds(30))
//                .reduce();
                //.trigger(new SegmentingOutOfOrderTrigger())
//                .process(new CalcImplementation());


        //DataStream<String> sideOutputStream = sum.getSideOutput(outputTag);

        //sideOutputStream.print();
        //execute program to see action
        env.execute("Streaming for Random Sampling");

    }

    /**
     * Alternative way to store data handled in state
     */
    public class ImplementationFields {

        public String key;
        public double sum;
        public double count;
        public double var;
        public double mean;
        //public long lastModified;
    }

    /**
     * The ProcessFunction can be thought of as a FlatMapFunction with access to keyed state and timers.
     * It handles events by being invoked for each event received in the input stream. Provides fine-grained control
     * over both state and time. Supports fault-tolerance using timers and timestamps
     */
    //TODO also if there is time implement fault tolerance https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/process_function.html
    //TODO add mean and variance
    public static class CalcImplementation extends KeyedProcessFunction<Tuple, Tuple2<String, Double>, Tuple5<String, Double, Double,Double,Double>> {

        /**
         * The ValueState handle. The first field is the key, the second field a running sum, the third a count of all elements.
         */
        private transient ValueState<Tuple7<String, Double,Double,Double,Double,Double,Double>> sum;
        //private transient ValueState<ImplementationFields> sum;

        @Override
        public void processElement(Tuple2<String, Double> input,Context ctx, Collector<Tuple5<String, Double, Double,Double,Double>> out) throws Exception {

            // access the state value
            Tuple7<String, Double,Double,Double,Double,Double,Double> currentSum = sum.value();
            //ImplementationFields currentSum = sum.value();

            if(currentSum == null){
                currentSum = Tuple7.of(input.f0,0.0D,0.0D,0.0D,0.0D,0.0D,0.0D);
            }
            // update the count
            currentSum.f2++;
            // add the second field of the input value
            currentSum.f1 += input.f1;
            //sum(xi^2)
            currentSum.f4 += Math.pow(input.f1,2);

            //mean(x)
            currentSum.f3 =  currentSum.f1/currentSum.f2;

            //mean(x^2)
            currentSum.f5 =  currentSum.f4/currentSum.f2;

            // CALC σ (standard deviation)
            currentSum.f6=Math.sqrt( (currentSum.f5-Math.pow(currentSum.f3,2)) );

            //System.out.println(" sum="+currentSum.f1+" count="+currentSum.f2+" mean="+currentSum.f3 +" x2="+currentSum.f4 +" mean2="+currentSum.f5+" σ ="+currentSum.f6);


            // update the state
            sum.update(currentSum);

            out.collect(new Tuple5<>(input.f0, currentSum.f1,currentSum.f2,currentSum.f3,currentSum.f6));
            //Added for side output implementation
            //ctx.output(outputTag, "sideout-" + String.valueOf(input.f0));
        }

        //Initialization inside ValueStateDescriptor is Deprecated. Must check null case and initialize in flatMap function
        @Override
        public void open(Configuration config) {

            //StateDescriptor holds name and characteristics of state
            ValueStateDescriptor<Tuple7<String, Double,Double,Double,Double,Double,Double>> descriptor = new ValueStateDescriptor<>(
                    "sum", // the state name
                    TypeInformation.of(new TypeHint<Tuple7<String, Double,Double,Double,Double,Double,Double>>() {})); // type information
            //Tuple3.of("",0.0F,0.0F)); // default value of the state, if nothing was set
            sum = getRuntimeContext().getState(descriptor);     //Access state using getRuntimeContext()

        }

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

    public static class WordsCapitalizer implements MapFunction<String, String> {
        @Override
        public String map(String s) {
            System.out.println(s.toUpperCase());
            return s.toUpperCase();
        }
    }

//    public static class SegmentingOutOfOrderTrigger extends Trigger<Tuple5<String,Double,Double,Double,Double>, GlobalWindow> {
//
//        @Override
//        public TriggerResult onElement(Tuple5<String,Double,Double,Double,Double> event, long timestamp, GlobalWindow window, TriggerContext context) throws Exception {
//
//            // if this is a stop event, set a timer
//            if (event.speed == 0.0) {
//                context.registerEventTimeTimer(event.timestamp);
//            }
//
//            return TriggerResult.CONTINUE;
//        }
//
//        @Override
//        public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) {
//            return TriggerResult.FIRE;
//        }
//
//        @Override
//        public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) {
//            return TriggerResult.CONTINUE;
//        }
//
//        @Override
//        public void clear(GlobalWindow window, TriggerContext ctx) {
//        }
//    }
//
//    public static class ConnectedCarAssigner implements AssignerWithPunctuatedWatermarks<Tuple5<String,Double,Double,Double,Double>> {
//        @Override
//        public long extractTimestamp(Tuple5<String,Double,Double,Double,Double> event, long previousElementTimestamp) {
//            return event.timestamp;
//        }
//
//        @Override
//        public Watermark checkAndGetNextWatermark(Tuple5<String,Double,Double,Double,Double> event, long extractedTimestamp) {
//            // simply emit a watermark with every event
//            return new Watermark(extractedTimestamp - 30000);
//        }
//    }


}