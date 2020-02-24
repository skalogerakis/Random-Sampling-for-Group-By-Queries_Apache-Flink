package tuc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import KafkaSchemas.*;
import java.util.*;


public class FirstAlgorithmPass {

    /**
     * NAME:
     * DESCRIPTION:This is the implementation of the first required job for our algorithm. In this job we parse data
     * for the first time (bounded stream) and compute required aggregation such as average, count for each stratum
     * (each stratum is formed by each distinct a group by attribute). We also compute values γi for each stratum
     * and γ(sum of γι for all stratum) which are required in the second pass of the algorithm
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        //TODO take all of those as parameters
        String inputTopic = "csvtokafka1";

        String inputNewTopic = "flinkout1";
        String outputTopic = "flinkaggr1";

        String consumerGroup = "KafkaCsvProducer";
        String address = "localhost:9092";

        //For OpenAq dataset
//        String example = "location,city,country,utc,local,parameter,value,unit,latitude,longitude,attribution";
//        String keys = "location";
//        String aggr = "value";

        //For population.csv
        String example = "Year,District.Code,District.Name,Neighborhood.Code,Neighborhood.Name,Gender,Age,Number";
        String keys = "District.Name";
        String aggr = "Number";


        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //parallelism definition
        env.setParallelism(4);

        //Used Ingestion time as time characteristic
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        //Used for dynamic key parsing from command line. More details in attrEval function below
        HashMap<String,List<Integer>> TotalAttrList = attrEval(example,keys,aggr);

        /**
         * IMPORTANT: Messages sent by a kafka producer to a particular topic partition will be appended in the order they are sent.
         */
        //---------------------------------------------------------------------------
        //                             KAFKA CONSUMERS
        //---------------------------------------------------------------------------

        FlinkKafkaConsumer<String> flinkKafkaConsumer = createStringConsumerForTopic(
                inputTopic, address, consumerGroup);
        //TODO see if we want to enable that
        flinkKafkaConsumer.setStartFromEarliest();


        //---------------------------------------------------------------------------
        //                             KAFKA PRODUCERS
        //---------------------------------------------------------------------------
        FlinkKafkaProducer<Tuple6<String,Double,Double,Double,Double,Double>> flinkKafkaProducer = createStringProducerAggr(
                outputTopic, address);
        flinkKafkaProducer.setWriteTimestampToKafka(true);

        FlinkKafkaProducer<Tuple3<String,Double,String>> flinkKafkaProducerInput = createStringProducerInput(
                inputNewTopic, address);
        flinkKafkaProducerInput.setWriteTimestampToKafka(true);

        //---------------------------------------------------------------------------
        //                             KAFKA PRODUCERS
        //---------------------------------------------------------------------------

        //Position of attributes, aggregation, keys
        List<Integer> keyPosList = TotalAttrList.get("key");
        List<Integer> attrPosList = TotalAttrList.get("attr");
        List<Integer> aggrPosList = TotalAttrList.get("aggr");

        //Take input from kafka and transform it in form Tuple3<String, Double,String>, <Keys, AggregationValues,Other Attributes>
        DataStream<Tuple3<String, Double,String>> inputTransformer = env.addSource(flinkKafkaConsumer)
                .flatMap(new FlatMapFunction<String, Tuple3<String,Double,String>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple3<String, Double,String>> out)
                            throws Exception {

                        //Create temp lists that hold all desired values
                        //After we find all of those values, concat them in a single field, comma seperated
                        List<String> tempkey = new ArrayList<String>();
                        List<String> tempattr = new ArrayList<String>();
                        //List<String> tempaggr = new ArrayList<String>();

                        //TODO works only for one element right now
                        //Currently works for one aggregation
                        int tempaggr = aggrPosList.get(0);

                        String[] words = value.split(",");
                        for(int i = 0; i< keyPosList.size(); i++){
                            tempkey.add(words[keyPosList.get(i)]);
                        }
                        String finalKey = String.join(",",tempkey);

                        for(int i = 0; i< attrPosList.size(); i++){
                            tempattr.add(words[attrPosList.get(i)]);
                        }
                        String finalAttr = String.join(",",tempattr);

                        Double finalAggr = Double.parseDouble(words[tempaggr]);

                        Tuple3<String,Double,String> temp1;

                        temp1 = new Tuple3<>(finalKey,finalAggr,finalAttr);
                        tempkey.clear();
                        tempattr.clear();
                        out.collect(temp1);

                    }
                });

        //Send transformed data into a new topic so that we can process them and not require the same procedure in job2
        inputTransformer.addSink(flinkKafkaProducerInput);

        //TODO change time to dynamic via input
        //First aggregate computations
        DataStream<Tuple6<String,Double,Double,Double,Double,String>> initAggr = inputTransformer
                .keyBy(0)
                .timeWindow(Time.seconds(50))
                .process(new initAggrWindow())
                ;
        initAggr.print();

        //Compute final gamma for all key by values. We use a dummy field to key by in this step.
        DataStream<Tuple5<String,Double,Double,Double,Double>> totalGammaAggr = initAggr
                .flatMap(new FlatMapFunction<Tuple6<String,Double,Double,Double,Double,String>, Tuple5<String,Double,Double,Double,Double>>() {
                    @Override
                    public void flatMap(Tuple6<String,Double,Double,Double,Double,String> value, Collector<Tuple5<String,Double,Double,Double,Double>> out)
                            throws Exception {
                        Tuple5<String,Double,Double,Double,Double> temp1 = new Tuple5<>("Total", value.f4,-1D,-1D,-1D);

                        out.collect(temp1);
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(50))
                .sum(1)
                ;

        totalGammaAggr.print();

        /**
         * As a final step we compute the final joined stream from the previous steps.
         * We join initAggr and totalGammaAggr streams in the dummy field "Total" that we created in both stream
         * The joined stream will include all the aggregations we want including gamma
         */
        DataStream<Tuple6<String,Double,Double,Double,Double,Double>> joinedStream= initAggr
                .join(totalGammaAggr)
                .where(new KeySelector<Tuple6<String,Double,Double,Double,Double,String>, String>() {
                    @Override
                    public String getKey(Tuple6<String,Double,Double,Double,Double,String> stringStringTuple6) throws Exception {
                        return stringStringTuple6.f5;
                    }
                })
                .equalTo(new KeySelector<Tuple5<String,Double,Double,Double,Double>, String>() {
                    @Override
                    public String getKey(Tuple5<String,Double,Double,Double,Double> stringDoubleDoubleDoubleTuple5) throws Exception {
                        return stringDoubleDoubleDoubleTuple5.f0;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(50)))
                //.allowedLateness(Time.seconds(10))
                .apply(new JoinFunction<Tuple6<String,Double,Double,Double,Double,String>, Tuple5<String,Double,Double,Double,Double>, Tuple6<String, Double, Double, Double, Double,Double>>() {
                    @Override
                    public Tuple6<String, Double, Double, Double, Double,Double> join(Tuple6<String,Double,Double,Double,Double,String> join1, Tuple5<String,Double,Double,Double,Double> join2) throws Exception {
                        return new Tuple6<>(join1.f0,join1.f1,join1.f2,join1.f3,join1.f4,join2.f1);
                    }
                })
                ;

        joinedStream.print();
        joinedStream.addSink(flinkKafkaProducer);

        //Print execution plan for visualisation purposes
        System.out.println(env.getExecutionPlan());

        //execute program to see action
        env.execute("Streaming for Random Sampling");

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

    public static FlinkKafkaProducer<Tuple6<String,Double,Double,Double,Double,Double>> createStringProducerAggr(
            String topic, String kafkaAddress){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);
        FlinkKafkaProducer<Tuple6<String,Double,Double,Double,Double,Double>> producer =  new FlinkKafkaProducer<Tuple6<String,Double,Double,Double,Double,Double>>(topic, new KafkaTestSchema(topic),props,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        return producer;
    }

    public static FlinkKafkaProducer<Tuple3<String,Double,String>> createStringProducerInput(
            String topic, String kafkaAddress){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);
        //TODO implemented based on this
        //https://github.com/luweizheng/flink-tutorials/blob/2a72c375d182cc47da016627023083ba85808f96/src/main/java/com/flink/tutorials/java/projects/wordcount/WordCountKafkaInKafkaOut.java
        //FlinkKafkaProducer producer = new FlinkKafkaProducer<>(topic, (SerializationSchema<Tuple5<String,Double,Double,Double,Double>>) new KafkaTestSchema(),props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        FlinkKafkaProducer<Tuple3<String,Double,String>> producer =  new FlinkKafkaProducer<Tuple3<String,Double,String>>(topic, new KafkaInputSchema(topic),props,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        return producer;
    }


    //TODO add checks when wrong input
    //TODO CHECK IF WE CAN HAVE MULTIPLE AGGREGATION. FOR NOW CAN BE USED ONLY WITH ONE
    /**
     * Name: attrEval
     * Description: Function that spots the location of keys, aggregation, attribute given by the user from command line interface.
     * @param attributes User must pass ALL the attributes parsed from .csv file in their EXACT order
     * @param keys User must pass all the fields of the desired keys(from attributes) which will be used for group by implementation later
     * @param aggregation User must pass the field for the aggregation(from attributes). FOR NOW ONLY ONE VALUE IS SUPPORTED FOR AGGREGATION
     * @return HashMap with locations of keys, aggregation from attributes
     */
    public static HashMap<String,List<Integer>> attrEval(String attributes, String keys, String aggregation){
        String[] attrSplitter = attributes.trim().split(",");
        String[] keySplitter = keys.trim().split(",");

        HashMap<String,List<Integer>> attrParser = new HashMap<String,List<Integer>>();
        //Make list for keys, attributes, aggregation respectively
        List<Integer> keyPosList = new ArrayList<Integer>();
        List<Integer> attrPosList = new ArrayList<Integer>();
        List<Integer> aggrPosList = new ArrayList<Integer>();


        for(int j=0; j<keySplitter.length;j++){
            for(int i=0; i<attrSplitter.length;i++){

                if(keySplitter[j].compareToIgnoreCase(attrSplitter[i])==0){
                    keyPosList.add(i);
                    continue;
                }else{
                    attrPosList.add(i);
                }

                if(attrSplitter[i].compareToIgnoreCase(aggregation)==0){
                    aggrPosList.add(i);
                }
            }
        }
        //Add all lists of locations to our hashmap
        attrParser.put("key",keyPosList);
        attrParser.put("attr",attrPosList);
        attrParser.put("aggr",aggrPosList);

        return attrParser;
    }



}


