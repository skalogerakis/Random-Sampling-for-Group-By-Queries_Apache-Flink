

## CSV_Parser
#### Description    
Class is responsible to parse data from a .csv file and write them to a certain Kafka topic


#### Arguments

|  Required  | Description |
| ------ | ------ |
| -csv-path | full path to the .csv we wish |
| -topic | Kafka topic to write data parsed from .csv |


|  Optional  | Description  | Default |
| ------ | ------ | ------ |
| -ip |  give ip for our server to work | localhost:9092 |
| -header-exists | Ignore first line when there is header option (0 when there is no header, 1 when there is header to ignore in csv | 1(header exists) |

## FirstAlgorithmPass
#### Description    

This is the implementation of the first required job for our algorithm. In this job we parse data for the first time (bounded stream) and compute required aggregation such as average, count for each stratum (each stratum is formed by each distinct a group by attribute). We also compute values γi for each stratum and γ(sum of γι for all stratum) which are required in the second pass of the algorithm

#### Arguments

|  Required  | Description |
| ------ | ------ |
| -all-attributes | All fields contained in the parsed .csv file(all csv fields) comma seperated |
| -keys | all keys to create stratum from group bys comma seperated(from attributes) |
| -aggr | field for aggregation from attributes(Currently only one is supported) |

|  Optional  | Description  | Default |
| ------ | ------ | ------ |
| -p | Parallelism in execution environment | 4 |
| -input-topic | Kafka topic that includes our main stream | input-topic-job1 |
| -output-topic | Kafka topic to export the aggregations required in the second pass | output-topic-job1 |
| -consumer-group | Kafka consumer group | KafkaCsvProducer |
| -ip | Give ip for our server to work | localhost:9092 |
| -windows-time | User-defined time for windows | 30 |

NOTE: User must ensure that there is adequate time in the time window for all entries to be processed

TODO my custom stream


## SecondAlgorithmPass
#### Description    
In this second pass of the algorithm, we parse once again the initial data stream. Also we use aggregation data extracted from first pass and combined we execute our demanded algorithm

|  Optional  | Description  | Default |
| ------ | ------ | ------ |
| -p | Parallelism in execution environment | 4 |
| -input-topic | Kafka topic that includes our main stream | input-topic-job1 |
| -output-topic | Kafka topic to export the final results after job2 executes | output-topic-job2 |
| -aggr-topic | Kafka topic to access the aggregations required in the second pass | output-topic-job2 |
| -consumer-group | Kafka consumer group | KafkaCsvProducer |
| -ip | Give ip for our server to work | localhost:9092 |
| -windows-time | User-defined time for windows | 30 |
| -M | Variable demanded in the algorithm | 20 |



## Paper
https://arxiv.org/pdf/1909.02629.pdf

## Execution Example on Linux
Kafka

Window 1
* Start zookeeper server
/<Kafka_path>/bin/zookeeper-server-start.sh /home/skalogerakis/kafka_2.12-2.4.0/config/zookeeper.properties

Window 2
* Start Kafka server
/home/skalogerakis/kafka_2.12-2.4.0/bin/kafka-server-start.sh /home/skalogerakis/kafka_2.12-2.4.0/config/server.properties

Now kafka servers are up and running waiting for new topics to be created.

Window 3
* Start flink cluster
/<flink_path>/bin/start-cluster.sh

To confirm that flink servers started successfully open any browser and type http://localhost:8081/

It should look something like that


![ ](/MyDocs/images/0.png)

Window 4

Open a new terminal in the project directory folder /ECE622/ and execute the following command to compile and build project

mvn clean package

BUILD SUCCESS message should show up. This commands generates a .jar files which will be used to execute our code from command line. Jar file can be found in the directory /ECE622/target/ECE622-1.0-SNAPSHOT.jar

The execution should start from CSV_Parser to parse data from a .csv file and append them to a kafka topic
/<flink_path>/bin/flink run -c utils.CSV_Parser <project_path>/ECE622/target/ECE622-1.0-SNAPSHOT.jar -csv-path <csv_path> -topic input-topic-job1 -p 4 

In our example, we use population.csv file located in /MyDocs/ directory. See sections above to check all available arguments for CSV_Parser.

Procedure finished message as shown below demonstrates that parser completed

![ ](/home/skalogerakis/Pictures/1.png)

NOTE: In order to avoid unexpected behaviour, wait for steps to complete and generate output

In order to check that kafka received our data we can use the following command

<kafka_path>/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <topic_name> --from-beginning;

We should see all the data contained in that specific topic. In our execution population.csv contains 70080 entries(excluding header) and as we can see all entries were parsed correctly.

![ ](/home/skalogerakis/Pictures/2.png)

Next step is to execute the first algorithm pass using the following command

/<flink_path>/bin/flink run -c tuc.FirstAlgorithmPass /<project_path>/ECE622/target/ECE622-1.0-SNAPSHOT.jar -all-attributes Year,District.Code,District.Name,Neighborhood.Code,Neighborhood.Name,Gender,Age,Number -keys District.Name,District.Code -aggr Number -p 4 -windows-time 60

In our example we use default topic input and output values. See sections above to check all available arguments for FirstAlgorithmPass.


As expected from our default topics we can see data in the topics output_topic_job1(aggregation values) and _input-topic-job1(Initial custom stream ) after execution

![ ](/home/skalogerakis/Pictures/3.png)
*Topic output-topic-job1*

![ ](/home/skalogerakis/Pictures/4.png)
*Topic _input-topic-job1*

Window 5

Final step is to execute the second algorithm pass using the following command

<flink_path>/bin/flink run -c tuc.SecondAlgorithmPass <project_path>/ECE622-1.0-SNAPSHOT.jar -p 4 -windows-time 60


In our example we use default topic input and output values. See sections above to check all available arguments for FirstAlgorithmPass.

We can see final output results in topic output-topic-job2

![ ](/home/skalogerakis/Pictures/6.png)

We can also verify our result from the web UI of flink. In the image below we notice our two jobs running succeessfuly

![ ](/home/skalogerakis/Pictures/5.png)

Each seperate job preserves metrics and statistics such as Records received/sent that match our desired behaviour

![ ](/home/skalogerakis/Pictures/8.png)
FirstAlgorithmPass

![ ](/home/skalogerakis/Pictures/7.png)
SecondAlgorithmPass

In addition, task manager preserves Stdout logs producing output we wish for both seperate jobs in one place

![ ](/home/skalogerakis/Pictures/9.png)


We also demonstrate for both jobs the plan visualizer https://flink.apache.org/visualizer/


![ ](/home/skalogerakis/Pictures/11.png)
FirstAlgorithmPass

![ ](/home/skalogerakis/Pictures/10.png)
SecondAlgorithmPass
