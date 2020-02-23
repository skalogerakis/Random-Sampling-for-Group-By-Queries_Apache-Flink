package test;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import java.util.Random;
import java.util.Vector;

public class RandomSampling2Phase {




    public static void main(String[] args) throws Exception {

double M =20.0D;

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
                        //key    count   γ      si
        DataStream<Tuple4<String,Double,Double,Double>> inputFrom1Phase = env
               .readTextFile("/home/john/TUC_Advanced_Database_System/MyDocs/phase2input")

               .flatMap(new FlatMapFunction<String, Tuple4<String,Double,Double,Double>>() {
            @Override
            public void flatMap(String value, Collector<Tuple4<String,Double,Double,Double>> out)
                    throws Exception {
                String[] words = value.split(",");
                double si = Math.round(M * (Double.parseDouble(words[2]) / Double.parseDouble(words[3])));//M*γi/γ
                //System.out.println(words[0]+" "+si);
                out.collect(new Tuple4<>(words[0], Double.parseDouble(words[1]), Double.parseDouble(words[3]), si));
            }});



        //TODO key,attributes
        DataStream<Tuple2<String,String>> input = env.readTextFile("/home/john/TUC_Advanced_Database_System/MyDocs/openaq_Bosnia2.csv")

                .flatMap(new FlatMapFunction<String, Tuple2<String,String>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String,String>> out)
                            throws Exception {
                        String[] words = value.split(",");
                        String arg= "";
                        for(int i=1; i<words.length;i++) {
                            if(i==1){
                            arg=words[i];}
                            else{
                                arg=arg+","+words[i];}
                        }

                        out.collect(new Tuple2<String,String>(words[0],arg));
                    }

                });


                          // key  args   γi     γ      si
        DataStream<Tuple5<String,String,Double,Double,Double>> joinedStream= input
                .join(inputFrom1Phase)
                .where(new KeySelector<Tuple2<String, String>, String>() {
                    @Override
                    public String getKey(Tuple2<String, String> stringStringTuple2) throws Exception {
                        return stringStringTuple2.f0;
                    }
                })
                .equalTo(new KeySelector<Tuple4<String, Double, Double, Double>, String>() {
                    @Override
                    public String getKey(Tuple4<String, Double, Double, Double> stringDoubleDoubleDoubleTuple4) throws Exception {
                        return stringDoubleDoubleDoubleTuple4.f0;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(2)))
                //.allowedLateness(Time.seconds(10))
                .apply(new JoinFunction<Tuple2<String, String>, Tuple4<String, Double, Double, Double>, Tuple5<String, String, Double, Double, Double>>() {
                    @Override
                    public Tuple5<String, String, Double, Double, Double> join(Tuple2<String, String> stringStringTuple2, Tuple4<String, Double, Double, Double> stringDoubleDoubleDoubleTuple4) throws Exception {
                        return new Tuple5<>(stringStringTuple2.f0,stringStringTuple2.f1,stringDoubleDoubleDoubleTuple4.f1,stringDoubleDoubleDoubleTuple4.f2,stringDoubleDoubleDoubleTuple4.f3);
                    }
                })
                ;






        DataStream<Tuple2<String,String>> sample =joinedStream

                .keyBy(0)
                .process(new ReservoirSampler())
                ;


        sample.keyBy(0).print();

        env.execute("Sampling");



    }//main





    public static class ReservoirSampler extends KeyedProcessFunction<Tuple,Tuple5<String,String,Double,Double,Double>, Tuple2<String,String>> {

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

        //System.out.println(input.productElement(0));
            if(s.sample.size()<input.f4){ // fill the sample vector with si elements
                Tuple2<String ,String> t = new Tuple2<String ,String>(input.f0,input.f1);
                s.sample.add(t);
                s.count++;// count how many elements have inserted
                stateSample.update(s);
            }
            else {

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

            if(s.count==input.f4){// if read all the expected tuples

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




}//class
