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


/**
 * Name:initAggrWindow
 * Description: Compute initial aggregation values for keyby values in a specified window. Stateful function.
 * Return: Type of Tuple6<String, Double, Double,Double,Double,String> with the following order
 *                        <Key, mean, Var, Count, Gamma(for each key by), Gamma_Fin(Gamma for entire window).
 *
 * NOTE: We cannot compute yet gamma_fin value as we need to sum all gamma values from all key by. We create a dummy field
 * with value "Total" that we can use later to join.
 *
 * The ProcessFunction can be thought of as a FlatMapFunction with access to keyed state and timers.
 * It handles events by being invoked for each event received in the input stream. Provides fine-grained control
 * over both state and time. Supports fault-tolerance using timers and timestamps
 */
public  class initAggrWindow extends ProcessWindowFunction<Tuple3<String, Double,String>, Tuple6<String, Double, Double,Double,Double,String>, Tuple, TimeWindow> {

    /**
     * The ValueState handle. The first field is the key, the second field a running sum, the third a count of all elements.
     */

    private transient ValueState<StateFieldsAggr> state;

    //TODO MAYbe take that from input
    final static int weight = 1;

    @Override
    public void process(Tuple key, Context ctx, Iterable<Tuple3<String, Double,String>> input, Collector<Tuple6<String, Double, Double,Double,Double,String>> out) throws Exception {

        //Everything is self-explanatory with the names used
        // access the state value
        StateFieldsAggr new_state = state.value();

        if(new_state == null){
            new_state = new StateFieldsAggr();
            new_state.__key = input.iterator().next().getField(0);
        }


        for (Tuple3<String, Double,String> in: input) {
            new_state = state.value();
            if(new_state == null){
                new_state = new StateFieldsAggr();
                new_state.__key = in.f0;
            }
            new_state.__gammaFin="Total";
            new_state.__count++;
            new_state.__sum+=in.f1;
            new_state.__sumSquares += Math.pow(in.f1, 2);
            new_state.__mean = new_state.__sum/new_state.__count;

            double tempMean = new_state.__sumSquares/ new_state.__count;

            new_state.__var=Math.sqrt(tempMean-Math.pow(new_state.__mean,2));

            //TODO maybe add that as parameter from out program
            new_state.__gamma = Math.sqrt(weight) * new_state.__var/new_state.__mean;

            state.update(new_state);
        }

        out.collect(new Tuple6<String,Double,Double,Double, Double,String>(input.iterator().next().getField(0),new_state.__mean,new_state.__var,new_state.__count,new_state.__gamma,new_state.__gammaFin));


    }


    @Override
    public void open(Configuration config) {
        //StateDescriptor holds name and characteristics of state
        ValueStateDescriptor<StateFieldsAggr> descriptor = new ValueStateDescriptor<>(
                "initAggr", // the state name
                TypeInformation.of(new TypeHint<StateFieldsAggr>() {})); // type information
        state = getRuntimeContext().getState(descriptor);//Access state using getRuntimeContext()
    }


}