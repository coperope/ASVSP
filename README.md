# ASVSP
Author: Petar Bašić
## Project architecture
![Architecture_sketch](/skica_arhitekture.png)
## Prerequisites 
- Docker 
- Docker compose

## Dataset 
The dataset used to simulate batch & stream processing in this project originated from [the Array of Things project](https://arrayofthings.github.io/index.html)
Data used while developing was data collected from sensor in city of Chicago and is available publicly on the following address:
https://aot-file-browser.plenar.io/data-sets/chicago-complete

## Batch processing
Batch processing of data in this project consists of calculating averages & minimum and maximum values of: temperature, particle concentration (gasses) and particle count (metals & other). Average values are calculater per each node and for a time periods of one month (or the whole dataset), for every hour and for every day. 
Min/Max values are calculated for every node(location of sensors) for every hour and for every day.
There has been a trial version of processing of number of people and cars detected on each node, but the data used did not contain real (or even good) values, so this kind of processing is commented out. 
### Preparing data
After downloading data for a specific time period, unzip the archive. 
Bring the containers up: 
```
cd docker_setup_batch
docker-compose up -d
```
Place the following files to the hdfs: data.csv, nodes.csv & sensors.csv:
```
docker exec -it namenode bash -c "hdfs dfs /path/to/local/data.csv/nodes.csv/sensors.csv /data"
```
### Running batch processing
Run the following commands 
```
cd docker_setup_batch
docker-compose up -d
cd ../batch-processing
docker cp process_sensor_data_batch.py spark-master:/process_sensor_data_batch.py
docker exec -it spark-master /bin/bash
$SPARK_HOME/bin/spark-submit process_sensor_data_batch.py
```
## Stream processing
Stream processing of data in this project uses spark-streaming & kafka as messaging service. A small python script emits prepared data to simulate real time sensor data.
There are two types of processing prepared: processing of the data from sesnors which measure air quality (particle concentration) & processing of the data coming from temperature & humidity sensors. 
Processing itself is simple and consists of detecting faulty sensors & high particle concentration. Detections can be seen as a warning or an error written in a command line. 

### Running stream processing
Run the following to initiate kafka service, spark workers and producer of the data.

```
cd stream-processing
docker-compose up --build
```
By default, producer will use the data containing concentrations of bad particles in the air (ppm_sensors_data.csv). 
In this default case there is a data consumer `ppm_consumer.py` which can be run by running:
```
cd consumer
docker cp ppm_consumer.py spark-master:/ppm_consumer.py
docker exec -it spark-master bash -c "$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 ppm_consumer.py zoo:2181 chicago_live"
```
To simulate additional source of data like temperature sensors, there is another consumer prepared: `main_consumer.py`. Data for this processing is not provided in this repository, but can be generated via a commented part of the main batch processing script. Please refer to the in line comments in the file `/batch-processing/process_sensor_data_batch.py`.




