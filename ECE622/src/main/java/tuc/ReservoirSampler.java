package tuc;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

public class ReservoirSampler extends KeyedProcessFunction<Tuple, Tuple5<String,String,Double,Double,Double>, Tuple2<String,String>> {

    /**
     * The ValueState handle. The first field is the key, the second field a running sum, the third a count of all elements.
     */

    private transient ValueState< StateSample > stateSample;

    @Override
    public void processElement(Tuple5<String,String,Double,Double,Double> input, Context ctx, Collector<Tuple2<String,String>> out) throws Exception {

        // access the state value
        StateSample s = stateSample.value();

        int getIn=0;
        if (s==null){ //state initialization
            s= new StateSample();
            s.count=0;
        }


        if(s.sample.size()<input.f4){ // fill the sample vector with si elements
            Tuple2<String ,String> t = new Tuple2<String ,String>(input.f0,input.f1);
            s.sample.add(t);
            s.count++;// count how many elements have inserted
            stateSample.update(s);
        }
        else if(s.count!=0){

            Random rand = new Random();
            int r = rand.nextInt(s.count); // generate number from 0 to inserted elements
            if (r < input.f4) {//si  get in with pr si/n
                getIn = 1;
            } // pr=si/n
            else {
                getIn = 0;
            }

            if(getIn==1){ // if get in =1 replace with pr 1/si
                //System.out.println(input.f0+" "+(int) Math.round(input.f4));
                r = rand.nextInt((int) Math.round(input.f4));//si
                s.getSample().set(r,new Tuple2<>(input.f0,input.f1));

            }

            s.count++; // readed tuples from stratum
            stateSample.update(s);
        }

        if(s.count==input.f4 && s.count!=0){// if read all the expected tuples

            for(int i=0; i<s.sample.size();i++){ //produce output with the sample

                out.collect(s.sample.get(i));
            }
        }

    }

    @Override
    public void open(Configuration config) {
        //StateDescriptor holds name and characteristics of state
        ValueStateDescriptor<StateSample> descriptor = new ValueStateDescriptor<>(
                "sum", // the state name
                TypeInformation.of(new TypeHint<StateSample>() {})); // type information
        stateSample = getRuntimeContext().getState(descriptor);//Access state using getRuntimeContext()
    }

}