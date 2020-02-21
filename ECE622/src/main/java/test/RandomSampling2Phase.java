package test;

import org.apache.commons.math3.geometry.euclidean.twod.Vector2D;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.sampling.ReservoirSamplerWithReplacement;
import org.apache.flink.api.java.sampling.ReservoirSamplerWithoutReplacement;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Enumeration;
import java.util.Random;
import java.util.Scanner;
import java.util.Vector;

import static java.lang.Double.valueOf;

public class RandomSampling2Phase {




    public static void main(String[] args) throws Exception {

double M =20.0D;

        //final Time timeout = Time.milliseconds(5);
        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        Vector <DataForSampling>dataForSampling = new Vector<DataForSampling>();
        try {
            File myObj = new File("/home/john/TUC_Advanced_Database_System/MyDocs/phase2input");
            Scanner myReader = new Scanner(myObj);
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();

                String[] words = data.split(",");
                DataForSampling d = new DataForSampling( words[0],Double.parseDouble(words[1]),Double.parseDouble(words[2]),Double.parseDouble(words[3]));
                d.setGamai( d.getSd()/d.getMean() );
                dataForSampling.addElement(d);

               // System.out.println(data);
            }
            myReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }

        double gama=0;
        for(int i=0; i<dataForSampling.size();i++){// calc sum(gamai)
            //System.out.println(dataForSampling.get(i).getKey()+" " + dataForSampling.get(i).getMean()+" " +dataForSampling.get(i).getSd()+" " +dataForSampling.get(i).getCount()+" " +dataForSampling.get(i).getGamai()+" ");
            gama+=dataForSampling.get(i).getGamai();
        }

        for(int i=0; i<dataForSampling.size();i++){// insert gama and si
            dataForSampling.get(i).setGama(gama);
            dataForSampling.get(i).setSi(Math.round( M*(dataForSampling.get(i).getGamai()/gama)) );
        }

        System.out.println("M="+M);
        for(int i=0; i<dataForSampling.size();i++){
            //System.out.println(dataForSampling.get(i).getKey()+" " + dataForSampling.get(i).getMean()+" " +dataForSampling.get(i).getSd()+" " +dataForSampling.get(i).getCount()+" " +dataForSampling.get(i).getGamai()+" " +dataForSampling.get(i).getGama()+" " +dataForSampling.get(i).getSi());
            System.out.println("key= "+dataForSampling.get(i).getKey()+" samples=" +dataForSampling.get(i).getSi());

        }



        DataStream<Tuple12<String,String,String,String,String,String, Double,String,Double,Double,String,DataForSampling>> input = env.readTextFile("/home/john/TUC_Advanced_Database_System/MyDocs/openaq_Bosnia2.csv")
                .flatMap(new FlatMapFunction<String, Tuple12<String,String,String,String,String,String, Double,String,Double,Double,String,DataForSampling>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple12<String,String,String,String,String,String, Double,String,Double,Double,String,DataForSampling>> out)
                            throws Exception {
                        String[] words = value.split(",");
                        DataForSampling d= new DataForSampling();

                        // insert data for sampling in every tuple
                        for(int i=0; i<dataForSampling.size();i++) {
                            if(dataForSampling.get(i).getKey().equals(words[0]))
                             d =dataForSampling.get(i);
                        }
                        Tuple12<String,String,String,String,String,String, Double,String,Double,Double,String,DataForSampling> temp1 = new Tuple12<>(words[0],words[1],words[2],words[3],words[4],words[5], Double.parseDouble(words[6]),words[7],Double.parseDouble(words[8]),Double.parseDouble(words[9]),words[10],d);
                        //tuple output
                        out.collect(temp1);
                    }
                });






        DataStream<Tuple11<String,String,String,String,String,String, Double,String,Double,Double,String>> sample =input
                .keyBy(0)
                .process(new ReservoirSampler())
                ;

        sample.keyBy(0).print();

        env.execute("Sampling");



    }//main


    public static class ReservoirSampler extends KeyedProcessFunction<Tuple,Tuple12<String,String,String,String,String,String, Double,String,Double,Double,String,DataForSampling>, Tuple11<String,String,String,String,String,String, Double,String,Double,Double,String>> {

        /**
         * The ValueState handle. The first field is the key, the second field a running sum, the third a count of all elements.
         */

        private transient ValueState< StateSample > stateSample;

        @Override
        public void processElement(Tuple12<String,String,String,String,String,String, Double,String,Double,Double,String,DataForSampling> input,Context ctx, Collector<Tuple11<String,String,String,String,String,String, Double,String,Double,Double,String>> out) throws Exception {

            // access the state value


            StateSample s = stateSample.value();
            int getIn=0;
            if (s==null){ //state initialization
                s= new StateSample();
                s.count=0;
            }


            if(s.sample.size()<input.f11.si){ // fill the sample vector with si elements
                Tuple11<String,String,String,String,String,String, Double,String,Double,Double,String> t = new Tuple11<>(input.f0,input.f1,input.f2,input.f3,input.f4,input.f5,input.f6,input.f7,input.f8,input.f9,input.f10);
                s.sample.add(t);
                s.count++;// count how many elements have inserted
                stateSample.update(s);
            }
            else {

                Random rand = new Random();
                int r = rand.nextInt(s.count); // generate number from 0 to inserted elements
                if (r < input.f11.si) {// get in with pr si/n
                    getIn = 1;
                } // pr=si/n
                else {
                    getIn = 0;
                }

                if(getIn==1){ // if get in =1 replace with pr 1/si
                    r = rand.nextInt((int) input.f11.si);
                    s.getSample().set(r,new Tuple11<>(input.f0,input.f1,input.f2,input.f3,input.f4,input.f5,input.f6,input.f7,input.f8,input.f9,input.f10));

                }

                s.count++; // readed tuples from stratum
                stateSample.update(s);
            }

            if(s.count==input.f11.count){// if read all the expected tuples

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
