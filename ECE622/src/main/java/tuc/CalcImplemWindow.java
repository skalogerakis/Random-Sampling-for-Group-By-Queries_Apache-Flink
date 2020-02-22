package tuc;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;



public  class CalcImplemWindow extends ProcessWindowFunction<Tuple2<String, Double>, Tuple5<String, Double, Double,Double,Double>, Tuple, TimeWindow> {

    /**
     * The ValueState handle. The first field is the key, the second field a running sum, the third a count of all elements.
     */

    private transient ValueState<Calculations> state;
//    private transient ValueState<Calculations> glob_state;
//    ValueStateDescriptor<Calculations> global_descriptor;
    final static int weight = 1;


    @Override
    public void process(Tuple key, Context ctx, Iterable<Tuple2<String, Double>> input, Collector<Tuple5<String, Double, Double,Double,Double>> out) throws Exception {

        // access the state value
        Calculations new_state = state.value();

        if(new_state == null){

            new_state = new Calculations();
            new_state.__key = input.iterator().next().getField(0);
        }

//        if(global_new_state == null){
//
//            global_new_state = new Calculations();
//            global_new_state.__key = input.iterator().next().getField(0);
//        }

        for (Tuple2<String, Double> in: input) {
            new_state = state.value();
            if(new_state == null){   //KEY COUNT MEAN MEAN2 SUM  SUM2 SD
                new_state = new Calculations();
                new_state.__key = in.f0;
            }
            //new_state.__gammaFin=-1;

            new_state.__count++;

            new_state.__sum+=in.f1;

            new_state.__sumSquares += Math.pow(in.f1, 2);

            new_state.__mean = new_state.__sum/new_state.__count;

            double tempMean = new_state.__sumSquares/ new_state.__count;

            //TODO this is variance and commented is standard deviation. Choose which one we want
            new_state.__var=(tempMean-Math.pow(new_state.__mean,2));
            //new_state.__var=Math.sqrt(tempMean-Math.pow(new_state.__mean,2)); SD

            //TODO maybe add that as parameter from out program
            new_state.__gamma = Math.sqrt(weight) * new_state.__var/new_state.__mean;

            state.update(new_state);
        }

        out.collect(new Tuple5<String,Double,Double,Double, Double>(input.iterator().next().getField(0),new_state.__mean,new_state.__var,new_state.__count,new_state.__gamma));


    }



    //Initialization inside ValueStateDescriptor is Deprecated. Must check null case and initialize in flatMap function
    @Override
    public void open(Configuration config) {

        //StateDescriptor holds name and characteristics of state
        ValueStateDescriptor<Calculations> descriptor = new ValueStateDescriptor<>(
                "sum", // the state name
                TypeInformation.of(new TypeHint<Calculations>() {})); // type information
        state = getRuntimeContext().getState(descriptor);//Access state using getRuntimeContext()


    }



}