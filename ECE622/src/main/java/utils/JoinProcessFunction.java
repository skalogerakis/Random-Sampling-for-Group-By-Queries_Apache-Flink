package utils;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import tuc.Calculations;

/**
 * TODO NO GOOD
 */
public class JoinProcessFunction  extends CoProcessFunction<Tuple5<String,Double,Double,Double,Double>,Tuple5<String,Double,Double,Double,Double>, Tuple6<String,Double,Double,Double,Double,Double>> {

    private transient ValueState<Calculations> state;

    @Override
    public void processElement1(Tuple5<String, Double, Double, Double, Double> input, Context context, Collector<Tuple6<String, Double, Double, Double, Double, Double>> collector) throws Exception {

        Calculations new_state = state.value();

        if(new_state == null){

            new_state = new Calculations();
            new_state.__key = input.f0;
            new_state.__mean = input.f1;
            new_state.__var = input.f2;
            new_state.__count = input.f3;
            new_state.__gamma = input.f4;

        }
        state.update(new_state);

    }

    @Override
    public void processElement2(Tuple5<String, Double, Double, Double, Double> input, Context context, Collector<Tuple6<String, Double, Double, Double, Double, Double>> collector) throws Exception {
        Calculations new_state = state.value();

        if(new_state == null){

            new_state = new Calculations();
            new_state.__gammaFin = input.f1;


        }
        new_state.__gammaFin = input.f1;
        state.update(new_state);
        collector.collect(new Tuple6<String, Double, Double, Double, Double, Double>(new_state.__key,new_state.__mean,new_state.__var,new_state.__count,new_state.__gamma,new_state.__gammaFin));
    }


    @Override
    public void open(Configuration parameters) throws Exception{

        //StateDescriptor holds name and characteristics of state
        ValueStateDescriptor<Calculations> descriptor = new ValueStateDescriptor<>(
                "sum", // the state name
                TypeInformation.of(new TypeHint<Calculations>() {})); // type information
        state = getRuntimeContext().getState(descriptor);//Access state using getRuntimeContext()
    }

}
