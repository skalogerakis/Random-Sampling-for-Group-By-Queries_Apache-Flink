

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

# SecondAlgorithmPass
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

