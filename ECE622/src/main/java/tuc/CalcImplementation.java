package tuc;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * The ProcessFunction can be thought of as a FlatMapFunction with access to keyed state and timers.
 * It handles events by being invoked for each event received in the input stream. Provides fine-grained control
 * over both state and time. Supports fault-tolerance using timers and timestamps
 */
//TODO also if there is time implement fault tolerance https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/process_function.html
public class CalcImplementation extends KeyedProcessFunction<Tuple, Tuple2<String,Double>, Tuple5<String, Double, Double,Double,Double>> {

    /**
     * The ValueState handle. The first field is the key, the second field a running sum, the third a count of all elements.
     */

    private transient ValueState<Calculations> state;

    @Override
    public void processElement(Tuple2<String,Double> input,Context ctx, Collector<Tuple5<String, Double, Double,Double,Double>> out) throws Exception {

        // access the state value
        Calculations new_state = state.value();


        if(new_state == null){
            //currentSum = Tuple7.of(input.f0,0.0D,0.0D,0.0D,0.0D,0.0D,0.0D);
            new_state = new Calculations();
            new_state.__key = input.f0+","+input.f1;
        }
        // update the count
        new_state.__count++;
        //currentSum.f2++;
        // add the second field of the input value
        //currentSum.f1 += input.f1;
        new_state.__sum+=input.f1;
        //sum(xi^2)
        //state.f4 += Math.pow(input.f1,2);
        new_state.__sumSquares +=  Math.pow(input.f1,2);
        //mean(x)
        //state.f3 =  state.f1/state.f2;
        new_state.__mean = new_state.__sum/new_state.__count;

        //mean(x^2)

        //state.f5 =  state.f4/state.f2;
        double tempMean = new_state.__sumSquares/ new_state.__count;

        //TODO ask if we want variance or standard deviation(standard deviation=sqrt(variance))
        // CALC σ (standard deviation)
        //currentSum.f6=Math.sqrt( (currentSum.f5-Math.pow(currentSum.f3,2)) );

        //variance
        //state.f6=(state.f5-Math.pow(state.f3,2));
        new_state.__var=(tempMean-Math.pow(new_state.__mean,2));
        //currentSum.f6 = /currentSum.f2;



        //System.out.println(" sum="+currentSum.f1+" count="+currentSum.f2+" mean="+currentSum.f3 +" x2="+currentSum.f4 +" mean2="+currentSum.f5+" σ ="+currentSum.f6);


        // update the state
        state.update(new_state);

        //out.collect(new Tuple5<>(input.f0, state.f1,state.f2,state.f3,state.f6));
        out.collect(new Tuple5<>(new_state.__key, new_state.__count,new_state.__sum,new_state.__mean,new_state.__var));
        //Added for side output implementation
        //ctx.output(outputTag, "sideout-" + String.valueOf(input.f0));
    }

    //Initialization inside ValueStateDescriptor is Deprecated. Must check null case and initialize in flatMap function
    @Override
    public void open(Configuration config) {

        //StateDescriptor holds name and characteristics of state
//        ValueStateDescriptor<Tuple7<String, Double,Double,Double,Double,Double,Double>> descriptor = new ValueStateDescriptor<>(
//                "sum", // the state name
//                TypeInformation.of(new TypeHint<Tuple7<String, Double,Double,Double,Double,Double,Double>>() {})); // type information
//        //Tuple3.of("",0.0F,0.0F)); // default value of the state, if nothing was set
//        sum = getRuntimeContext().getState(descriptor);     //Access state using getRuntimeContext()
        ValueStateDescriptor<Calculations> descriptor = new ValueStateDescriptor<>(
                "state", // the state name
                TypeInformation.of(new TypeHint<Calculations>() {})); // type information
        //Tuple3.of("",0.0F,0.0F)); // default value of the state, if nothing was set
        state = getRuntimeContext().getState(descriptor);     //Access state using getRuntimeContext()

    }

}