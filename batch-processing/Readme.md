## Running batch processing
```
cd ../docker_setup_batch
docker-compose up -d
cd ../batch-processing
docker cp process_sensor_data_batch.py spark-master:/process_sensor_data_batch.py
docker exec -it spark-master /bin/bash
$SPARK_HOME/bin/spark-submit process_sensor_data_batch.py
```