package tuc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


public class RandomSampling {

    //final static OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();



        //DataStream<Tuple2<String, Float>> csvInput = env.readTextFile("/home/skalogerakis/TUC_Projects/TUC_Advanced_Database_Systems/MyDocs/openaq_Bosnia.csv")

        //This is for data input purposes only. TODO replace that with Kafka implementation
        //Current way data are transformed Tuple2<String, Float>
        DataStream<Tuple2<String, Float>> input = env.readTextFile("/home/skalogerakis/Downloads/openaq.csv")
                .flatMap(new FlatMapFunction<String, Tuple2<String, Float>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Float>> out)
                    throws Exception {
                String[] words = value.split(",");
                Tuple2<String, Float> temp1 = new Tuple2<>(words[0], Float.parseFloat(words[6]));
                out.collect(temp1);
            }
        });

        //FlatMap implementation
        //DataStream<Tuple3<String, Float,Float>> sum = input.keyBy(0).flatMap(new CalcImplementation());

        //KeyedProcessFunction implementation
        DataStream<Tuple3<String, Float,Float>> sum = input
                .keyBy(0)
                .process(new CalcImplementation());
        sum.print();

        //Using side stream
//        SingleOutputStreamOperator<Tuple3<String, Float,Float>> sum = input
//                .keyBy(0)
//                .process(new CalcImplementation());



        //sum.print();

        //DataStream<String> sideOutputStream = sum.getSideOutput(outputTag);

        //sideOutputStream.print();
        //execute program to see action
        env.execute("Streaming for Random Sampling");

    }

//    //TODO add mean and variance
//    public static class CalcImplementation extends RichFlatMapFunction<Tuple2<String, Float>, Tuple3<String, Float, Float>> {
//
//        /**
//         * The ValueState handle. The first field is the key, the second field a running sum, the third a count of all elements.
//         */
//        private transient ValueState<Tuple3<String, Float,Float>> sum;
//
//        @Override
//        public void flatMap(Tuple2<String, Float> input, Collector<Tuple3<String, Float, Float>> out) throws Exception {
//
//            // access the state value
//            Tuple3<String, Float,Float> currentSum = sum.value();
//
//            if(currentSum == null){
//                currentSum = Tuple3.of(input.f0,0.0F,0.0F);
//            }
//            // update the count
//            currentSum.f2++;
//            // add the second field of the input value
//            currentSum.f1 += input.f1;
//
//            // update the state
//            sum.update(currentSum);
//
//            out.collect(new Tuple3<>(input.f0, currentSum.f1,currentSum.f2));
//        }
//
//        //Initialization inside ValueStateDescriptor is Deprecated. Must check null case and initialize in flatMap function
//        @Override
//        public void open(Configuration config) {
//
//            //StateDescriptor holds name and characteristics of state
//            ValueStateDescriptor<Tuple3<String, Float,Float>> descriptor = new ValueStateDescriptor<>(
//                            "sum", // the state name
//                            TypeInformation.of(new TypeHint<Tuple3<String, Float,Float>>() {})); // type information
//                            //Tuple3.of("",0.0F,0.0F)); // default value of the state, if nothing was set
//            sum = getRuntimeContext().getState(descriptor);     //Access state using getRuntimeContext()
//        }
//
//    }

    /**
     * Alternative way to store data handled in state
     */
    public class ImplementationFields {

        public String key;
        public float sum;
        public float count;
        //public long lastModified;
    }

    /**
     * The ProcessFunction can be thought of as a FlatMapFunction with access to keyed state and timers.
     * It handles events by being invoked for each event received in the input stream. Provides fine-grained control
     * over both state and time. Supports fault-tolerance using timers and timestamps
     */
    //TODO also if there is time implement fault tolerance https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/process_function.html
    //TODO add mean and variance
    public static class CalcImplementation extends KeyedProcessFunction<Tuple, Tuple2<String, Float>, Tuple3<String, Float, Float>> {

        /**
         * The ValueState handle. The first field is the key, the second field a running sum, the third a count of all elements.
         */
        private transient ValueState<Tuple3<String, Float,Float>> sum;
        //private transient ValueState<ImplementationFields> sum;

        @Override
        public void processElement(Tuple2<String, Float> input,Context ctx, Collector<Tuple3<String, Float, Float>> out) throws Exception {

            // access the state value
            Tuple3<String, Float,Float> currentSum = sum.value();
            //ImplementationFields currentSum = sum.value();

            if(currentSum == null){
                currentSum = Tuple3.of(input.f0,0.0F,0.0F);
            }
            // update the count
            currentSum.f2++;
            // add the second field of the input value
            currentSum.f1 += input.f1;

            // update the state
            sum.update(currentSum);

            out.collect(new Tuple3<>(input.f0, currentSum.f1,currentSum.f2));
            //Added for side output implementation
            //ctx.output(outputTag, "sideout-" + String.valueOf(input.f0));
        }

        //Initialization inside ValueStateDescriptor is Deprecated. Must check null case and initialize in flatMap function
        @Override
        public void open(Configuration config) {

            //StateDescriptor holds name and characteristics of state
            ValueStateDescriptor<Tuple3<String, Float,Float>> descriptor = new ValueStateDescriptor<>(
                    "sum", // the state name
                    TypeInformation.of(new TypeHint<Tuple3<String, Float,Float>>() {})); // type information
            //Tuple3.of("",0.0F,0.0F)); // default value of the state, if nothing was set
            sum = getRuntimeContext().getState(descriptor);     //Access state using getRuntimeContext()

        }

    }



}
