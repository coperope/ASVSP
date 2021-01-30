# Stream processing simulation

This simulation shows analysis of stream data from multiple sources.
To change source: change line 23 in /producer/main_produces.py
```
f = csv.reader(open("ppm_seonsors_data.csv"), delimiter=",")
```
Replace `ppm_seonsors_data.csv ` with `temperature_sensors.data.csv`

## Running simulation

run the following to initiate kafka service, spark workers and producer of the data.

```
docker-compose up --build
```

Consumer application is run seperately and can be one of ppm_consumer or main_consumer depending on which source of data was set.

To run a consumer (replace ppm_consumer.py if different source was chosen):

```
cd consumer
docker cp ppm_consumer.py spark-master:/ppm_consumer.py
docker exec -it spark-master bash -c "$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 ppm_consumer.py zoo:2181 chicago_live"
```