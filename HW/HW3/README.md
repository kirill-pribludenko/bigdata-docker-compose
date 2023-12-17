## Kafka
Example How to run Task 1:
```
py producer.py

#in another cli
docker-compose exec jobmanager ./bin/flink run -py /opt/pyflink/device_job_task1.py -d
py consumer_task1.py
```
Other tasks are similar

## Result
Task 1 - I couldn't make save checkpoint to hdfs, most likely problem is somewhere in the docker configuration
Task 2 - For each window need to uncomment corresponding part of code
Task 3 - I did it as I understood, I did not understand from the conditions where to send requests and to which database to save 


## Tips Command

```commandline
docker-compose build
```
```commandline
docker-compose up -d
```
```commandline
docker-compose ps
```
```
http://localhost:8081/#/overview
```
```commandline
docker-compose down -v
```
```commandline
docker-compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --topic topic_name --partitions 1 --replication-factor 1
```
```commandline
docker-compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --describe itmo  
```
```commandline
 docker-compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --alter --topic itmo --partitions 2
```
```commandline
docker-compose exec jobmanager ./bin/flink run -py /opt/pyflink/device_job.py -d  
```
