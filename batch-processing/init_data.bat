docker exec namenode bash -c "hdfs dfs -mkdir /data"
docker exec namenode bash -c "hdfs dfs -put /data/* /data"