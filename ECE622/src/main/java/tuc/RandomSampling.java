package tuc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class RandomSampling {

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //DataStream<Tuple2<String, Float>> csvInput = env.readTextFile("/home/skalogerakis/TUC_Projects/TUC_Advanced_Database_Systems/MyDocs/openaq_Bosnia.csv")
//        DataStream<Tuple2<String, Float>> csvInput = env.readTextFile("/home/skalogerakis/Downloads/openaq.csv")
//                .map(new MapFunction<String, Tuple2<String, Float>>() {
//                    @Override
//                    public Tuple2<String, Float> map(String value) throws Exception {
//                        String[] words = value.split(",");
//                        Tuple2<String, Float> t1 = new Tuple2<>(words[0], Float.parseFloat(words[6]));
//                        return t1;
//                    }
//                });

        DataStream<Tuple2<String, Float>> csvInput = env.readTextFile("/home/skalogerakis/Downloads/openaq.csv")
                .flatMap(new FlatMapFunction<String, Tuple2<String, Float>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Float>> out)
                    throws Exception {
                String[] words = value.split(",");
                Tuple2<String, Float> t1 = new Tuple2<>(words[0], Float.parseFloat(words[6]));
                out.collect(t1);
            }
        });

        DataStream<Tuple2<String, Float>> sum = csvInput.keyBy(0).sum(1);

        sum.print();

        //execute program to see action
        env.execute("Streaming for Random Sampling");

    }

//    public class CountWindowAverage extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Long>> {
//
//        /**
//         * The ValueState handle. The first field is the count, the second field a running sum.
//         */
//        private transient ValueState<Tuple2<Long, Long>> sum;
//
//        @Override
//        public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {
//
//            // access the state value
//            Tuple2<Long, Long> currentSum = sum.value();
//
//            // update the count
//            currentSum.f0 += 1;
//
//            // add the second field of the input value
//            currentSum.f1 += input.f1;
//
//            // update the state
//            sum.update(currentSum);
//
//            // if the count reaches 2, emit the average and clear the state
//            if (currentSum.f0 >= 2) {
//                out.collect(new Tuple2<>(input.f0, currentSum.f1 / currentSum.f0));
//                sum.clear();
//            }
//        }
//
//        @Override
//        public void open(Configuration config) {
//            ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
//                    new ValueStateDescriptor<>(
//                            "average", // the state name
//                            TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}), // type information
//                            Tuple2.of(0L, 0L)); // default value of the state, if nothing was set
//            sum = getRuntimeContext().getState(descriptor);
//        }
//    }

}
