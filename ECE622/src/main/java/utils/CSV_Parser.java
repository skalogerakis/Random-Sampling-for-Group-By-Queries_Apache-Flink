package utils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.logging.*;
import java.util.stream.Stream;
import java.sql.Timestamp;



import static java.lang.System.exit;

public class CSV_Parser {

    //in our machine will run on localhost
    private static String KafkaBrokerEndpoint = null;
    //this is the name of our kafka topic
    private static String KafkaTopic = null;
    private static String CsvFile = null;
    private static int headerExists = 1;
    private static final Logger LOGGER = Logger.getLogger(CSV_Parser.class.getName());


    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    /**
     * -------------------------------------------------------------------------------------------------------------------
     * Name:CSV_Parser
     * Description: This class is responsible to parse data from a .csv file and write them to a certain Kafka topic
     * @param args ARGUMENTS: -csv-path <csv_path_file> -topic <KafkaTopic> -ip <KafkaBrokerEndPoint>(Optional) -header-exists <headerExists>(Optional) (0 when there is no
     *             header, 1 when there is header to ignore in csv[default])
     * @throws IOException
     * -------------------------------------------------------------------------------------------------------------------
     */
    public static void main(String[] args) throws IOException {

        ParameterTool parameterTool=null;

        try{
            parameterTool = ParameterTool.fromArgs(args);
        }catch(IllegalArgumentException io){
            System.out.println("Error while parsing arguments. Please prefix keys with -- or -. ARGUMENTS: -csv-path <csv_path_file> -topic <KafkaTopic> -ip <KafkaBrokerEndPoint>(Optional) -header-exists <headerExists>(Optional)");
            System.exit(-1);
        }
        //Create a logger for debugging purposes
        CSV_Parser csvParser = new CSV_Parser();
        csvParser.LogConfig();

        try{
            CsvFile = parameterTool.getRequired("csv-path");///csv full path
            KafkaTopic = parameterTool.getRequired("topic");//Kafka to write our data
        }catch (RuntimeException re){
            System.out.println("Required field not given. ARGUMENTS: -csv-path <csv_path_file> -topic <KafkaTopic> -ip <KafkaBrokerEndPoint>(Optional) -header-exists <headerExists>(Optional)");
            System.exit(-1);
        }


        try{
            headerExists = Integer.parseInt(parameterTool.get("header-exists"));
        }catch (NumberFormatException ne){
            System.out.println("Invalid headerExists option. Proceed with the default option(Header exists)");
        }

        KafkaBrokerEndpoint = parameterTool.get("ip");
        if(KafkaBrokerEndpoint==null){
            KafkaBrokerEndpoint = BOOTSTRAP_SERVERS;//default option
        }


        Properties props = new Properties();
        csvParser.propConfig(props);

        KafkaProducer<String, String> producer = null;

        try{
            producer = new KafkaProducer<>(props);
        }catch(KafkaException ke){
            LOGGER.log(Level.SEVERE, "Failed to construct kafka producer. Check that url provided is correct.Terminating.....",ke);
            exit(-1);
        }

        try{
            Stream<String> FileStream = Files.lines(Paths.get(CsvFile));

            KafkaProducer<String, String> finalProducer = producer;
            FileStream.forEach(line -> {
                /**
                 * Ignores first line when there is a header in our csv to ignore. Default value does not
                 * ignore first line
                 */
                if(headerExists==1){
                    System.out.println("Ignore first header line");
                    headerExists=0;
                    return;
                }

                /**
                 * Synchronous Kafka producer to make sure we will not lose any data
                 */
                try {
                    RecordMetadata metadata = finalProducer.send(new ProducerRecord<>(KafkaTopic, line)).get();
                    LOGGER.log(Level.INFO,("Message " + line + " persisted with offset " + metadata.offset()
                            + " and timestamp on " + new Timestamp(metadata.timestamp())));
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Exception occured",e);
                    throw new RuntimeException(e);
                }

            });
            /**
             * Final message sent after our stream terminated.CURRENTLY NOT USED
             */
            finalProducer.flush();
            finalProducer.close();
        }catch (IOException io){
            //Exception occured
            LOGGER.log(Level.SEVERE, "Exception occured. Exiting....",io);
            io.printStackTrace();
        }

        System.out.println("Procedure finished");
        //close kafka producer
        producer.flush();
        producer.close();
    }

    public void LogConfig() throws IOException {

        //Creating consoleHandler and fileHandler
        Handler consoleHandler = new ConsoleHandler();
        Handler fileHandler  = new FileHandler("./Logfile.log",false);

        //Assigning handlers to LOGGER object
        LOGGER.addHandler(consoleHandler);
        LOGGER.addHandler(fileHandler);

        //Setting levels to handlers and LOGGER
        consoleHandler.setLevel(Level.ALL);
        fileHandler.setLevel(Level.ALL);
        LOGGER.setLevel(Level.ALL);

        LOGGER.config("Configuration done.");

        //Console handler removed
        LOGGER.removeHandler(consoleHandler);
        LOGGER.setUseParentHandlers(false);
    }

    public void propConfig(Properties props){

        /**
         * Configure properties
         * BOOTSTRAP_SERVERS_CONFIG: Servers that Producer uses to establish initial connection
         * CLIENT_ID_CONFIG: ID to pass to the server when making requests so the server can track the source of requests
         */
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaCsvProducer");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaBrokerEndpoint);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //Set acknowledgements for producer requests.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        
    }

}


