package utils;

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
    private static final Logger LOGGER = Logger.getLogger(CSV_Parser.class.getName());


    /**
     *  TODO set a default if not given by the user
     *  TODO check for input
     *  TODO support JSON format and check header case with csv
     *
     *
     */

    private final static String BOOTSTRAP_SERVERS =
            "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) throws IOException {


        CSV_Parser csvParser = new CSV_Parser();
        csvParser.LogConfig();

        if (args != null){
            KafkaBrokerEndpoint = args[0];
            KafkaTopic = args[1];
            CsvFile = args[2];
        }

        /**
         * Configure properties
         * BOOTSTRAP_SERVERS_CONFIG: Servers that Producer uses to establish initial connection
         * CLIENT_ID_CONFIG: ID to pass to the server when making requests so the server can track the source of requests
         */
        Properties props = new Properties();

        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaCsvProducer");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaBrokerEndpoint);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //Set acknowledgements for producer requests.
        props.put("acks", "all");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //If the request fails, the producer can automatically retry,
        //props.put("retries", 0);
        props.put(ProducerConfig.RETRIES_CONFIG, 0);

        KafkaProducer<String, String> producer = null;

        try{
            producer = new KafkaProducer<>(props);
        }catch(KafkaException ke){
            LOGGER.log(Level.SEVERE, "Failed to construct kafka producer. Check that url provided is correct.Terminating.....",ke);
            exit(-1);
        }


        try{
            Stream<String> FileStream = Files.lines(Paths.get(CsvFile));
            //System.out.println(Files.lines(Paths.get(CsvFile)).count());
            KafkaProducer<String, String> finalProducer = producer;
            FileStream.forEach(line -> {
                //System.out.println(line.toString());
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
             * Final message sent after our stream terminated
             */
            finalProducer.send(new ProducerRecord<>(KafkaTopic, "EndOfStream"));
            finalProducer.flush();
            finalProducer.close();
        }catch (IOException io){
            //Exception occured
            LOGGER.log(Level.SEVERE, "Exception occured",io);
            io.printStackTrace();
        }

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

}


