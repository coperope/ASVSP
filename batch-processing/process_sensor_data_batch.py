#!/usr/bin/python
### before spark-submit: export PYTHONIOENCODING=utf8

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import *

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

conf = SparkConf().setAppName("uni").setMaster("local")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

quiet_logs(spark)
# #------------------------------------------------- NODES DATA SCHEMA ------------------------------------------------------------------
schemaString = "timestamp node_id subsystem sensor parameter value_raw value_hrf"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

df = spark.read.csv("hdfs://namenode:9000/data/data2.csv", header=True, mode="DROPMALFORMED", schema=schema)

df = df.withColumn("value_raw", df["value_raw"].cast(FloatType()))
df = df.withColumn("value_hrf", df["value_hrf"].cast(FloatType()))
# Prepare timestamp for correct sql execution.
df = df.withColumn("timestamp", regexp_replace(col("timestamp"), "/", "-"))
# Reduce the dataset.
# df = df.filter(col("timestamp") > "2020-09-25 00:00:00")
df.printSchema()
df.createOrReplaceTempView("nodes")
# #------------------------------------------------- SENSORS SCHEMA ------------------------------------------------------------------
sensorsSchemaString = "ontology subsystem sensor parameter hrf_unit hrf_minval hrf_maxval datasheet"
sensorsFields = [StructField(field_name, StringType(), True) for field_name in sensorsSchemaString.split()]
sensorsSchema = StructType(sensorsFields)
dfSensors = spark.read.csv("hdfs://namenode:9000/data/sensors.csv", header=True, mode="DROPMALFORMED", schema=sensorsSchema)

dfSensors = dfSensors.withColumn("hrf_minval", dfSensors["hrf_minval"].cast(IntegerType()))
dfSensors = dfSensors.withColumn("hrf_maxval", dfSensors["hrf_maxval"].cast(IntegerType()))
dfSensors.createOrReplaceTempView("sensors")
# #------------------------------------------------- LOCATION SCHEMA ------------------------------------------------------------------
locationSchemaString = "node_id project_id vsn address lat lon start_timestamp end_timestamp"
locationFields = [StructField(field_name, StringType(), True) for field_name in locationSchemaString.split()]
locationSchema = StructType(locationFields)
dfLocation = spark.read.csv("hdfs://namenode:9000/data/nodes.csv", header=True, mode="DROPMALFORMED", schema=locationSchema)

dfLocation = dfLocation.withColumn("lat", dfLocation["lat"].cast(FloatType()))
dfLocation = dfLocation.withColumn("lon", dfLocation["lon"].cast(FloatType()))
dfLocation.createOrReplaceTempView("locations")
# #--------------------------------------------------- Queries ----------------------------------------------------------------------

# #--------------------------------------------------- Averages per month  ----------------------------------------------------------

queryForAverageTemperature = "SELECT n.node_id, n.parameter, count(*) NumberOfMeasurments, \
                      n.sensor NodesTableSensor, \
                      s.hrf_unit UnitOfMeasurment, \
                      first(l.address) Address,\
                      first(l.lat) Latitude,\
                      first(l.lon) Longitude,\
                      round(sum(n.value_hrf)/count(*),2) AverageTemperature \
                      FROM nodes n LEFT JOIN sensors s on  n.sensor=s.sensor\
                      LEFT JOIN locations l on n.node_id=l.node_id\
                      WHERE n.parameter = 'temperature' AND n.sensor = 'pr103j2' \
                      GROUP BY n.node_id, n.parameter, s.sensor, n.sensor, s.hrf_unit \
                      ORDER BY n.node_id"

queryForAverageConcentration = "SELECT n.node_id, n.parameter, count(*) NumberOfMeasurments, \
                       n.sensor NodesTableSensor, \
                       s.hrf_unit UnitOfMeasurment, \
                       first(l.address) Address,\
                       first(l.lat) Latitude,\
                       first(l.lon) Longitude,\
                       round(sum(n.value_hrf)/count(*),2) AverageConcentration \
                       FROM nodes n LEFT JOIN sensors s on  n.sensor=s.sensor\
                       LEFT JOIN locations l on n.node_id=l.node_id\
                       WHERE n.parameter = 'concentration'\
                       GROUP BY n.node_id, n.parameter, s.sensor, n.sensor, s.hrf_unit \
                       ORDER BY n.node_id"

# #--------------------------------------------------- Averages per Hour  ----------------------------------------------------------
queryForAverageTemperaturePerHour = "SELECT HOUR(n.timestamp) as timeHour, first(n.timestamp), n.node_id, n.parameter, count(*) NumberOfMeasurments, \
                      n.sensor NodesTableSensor, \
                      s.hrf_unit UnitOfMeasurment, \
                      first(l.address) Address,\
                      first(l.lat) Latitude,\
                      first(l.lon) Longitude,\
                      round(sum(n.value_hrf)/count(*),2) AverageTemperature \
                      FROM nodes n LEFT JOIN sensors s on  n.sensor=s.sensor\
                      LEFT JOIN locations l on n.node_id=l.node_id\
                      WHERE n.parameter = 'temperature' AND n.sensor = 'pr103j2' \
                      GROUP BY n.node_id, n.parameter, s.sensor, n.sensor, s.hrf_unit, timeHour \
                      ORDER BY n.node_id, timeHour"

queryForAverageConcentrationPerHour = "SELECT HOUR(n.timestamp) as timeHour, first(n.timestamp), n.node_id, n.parameter, count(*) NumberOfMeasurments, \
                      n.sensor NodesTableSensor, \
                      s.hrf_unit UnitOfMeasurment, \
                      first(l.address) Address,\
                      first(l.lat) Latitude,\
                      first(l.lon) Longitude,\
                      round(sum(n.value_hrf)/count(*),2) AverageConcentration \
                      FROM nodes n LEFT JOIN sensors s on  n.sensor=s.sensor\
                      LEFT JOIN locations l on n.node_id=l.node_id\
                      WHERE n.parameter = 'concentration'\
                      GROUP BY n.node_id, n.parameter, s.sensor, n.sensor, s.hrf_unit, timeHour \
                      ORDER BY n.node_id, timeHour"

queryForAverageParticleCountPerHour = "SELECT HOUR(n.timestamp) as timeHour, first(n.timestamp), n.node_id, n.parameter, count(*) NumberOfMeasurments, \
                      n.sensor NodesTableSensor, \
                      s.hrf_unit UnitOfMeasurment, \
                      first(l.address) Address,\
                      first(l.lat) Latitude,\
                      first(l.lon) Longitude,\
                      round(sum(n.value_hrf)/count(*),2) MaxParticleCount \
                      FROM nodes n LEFT JOIN sensors s on  n.sensor=s.sensor\
                      LEFT JOIN locations l on n.node_id=l.node_id\
                      WHERE s.hrf_unit = 'μg/m^3'\
                      GROUP BY n.node_id, n.parameter, s.sensor, n.sensor, s.hrf_unit, timeHour \
                      ORDER BY n.node_id, timeHour"

# #--------------------------------------------------- MinMax per hour  ----------------------------------------------------------

queryForMaxMinTempPerHour = "SELECT HOUR(n.timestamp) as timeHour, first(n.timestamp), n.node_id, n.parameter, count(*) NumberOfMeasurments, \
                      n.sensor NodesTableSensor, \
                      s.hrf_unit UnitOfMeasurment, \
                      first(l.address) Address,\
                      first(l.lat) Latitude,\
                      first(l.lon) Longitude,\
                      max(n.value_hrf) MaxTemperature, \
                      min(n.value_hrf) MinTemperature  \
                      FROM nodes n LEFT JOIN sensors s on  n.sensor=s.sensor\
                      LEFT JOIN locations l on n.node_id=l.node_id\
                      WHERE n.parameter = 'temperature' AND n.sensor = 'pr103j2'\
                      GROUP BY n.node_id, n.parameter, s.sensor, n.sensor, s.hrf_unit, timeHour \
                      ORDER BY n.node_id, timeHour"

queryForMaxMinConcPerHour = "SELECT HOUR(n.timestamp) as timeHour, first(n.timestamp), n.node_id, n.parameter, count(*) NumberOfMeasurments, \
                      n.sensor NodesTableSensor, \
                      s.hrf_unit UnitOfMeasurment, \
                      first(l.address) Address,\
                      first(l.lat) Latitude,\
                      first(l.lon) Longitude,\
                      max(n.value_hrf) MaxConcentration, \
                      min(n.value_hrf) MinConcentration \
                      FROM nodes n LEFT JOIN sensors s on  n.sensor=s.sensor\
                      LEFT JOIN locations l on n.node_id=l.node_id\
                      WHERE n.parameter = 'concentration'\
                      GROUP BY n.node_id, n.parameter, s.sensor, n.sensor, s.hrf_unit, timeHour \
                      ORDER BY n.node_id, timeHour"

queryForMaxMinParticleCountPerHour = "SELECT HOUR(n.timestamp) as timeHour, first(n.timestamp), n.node_id, n.parameter, count(*) NumberOfMeasurments, \
                      n.sensor NodesTableSensor, \
                      s.hrf_unit UnitOfMeasurment, \
                      first(l.address) Address,\
                      first(l.lat) Latitude,\
                      first(l.lon) Longitude,\
                      max(n.value_hrf) MaxParticleCount, \
                      min(n.value_hrf) MinParticleCount \
                      FROM nodes n LEFT JOIN sensors s on  n.sensor=s.sensor\
                      LEFT JOIN locations l on n.node_id=l.node_id\
                      WHERE s.hrf_unit = 'μg/m^3'\
                      GROUP BY n.node_id, n.parameter, s.sensor, n.sensor, s.hrf_unit, timeHour \
                      ORDER BY n.node_id, timeHour"  

# #--------------------------------------------------- MinMax per day  ----------------------------------------------------------

queryForMaxMinTempPerDay = "SELECT DAY(n.timestamp) as timeDay, first(n.timestamp), n.node_id, n.parameter, count(*) NumberOfMeasurments, \
                      n.sensor NodesTableSensor, \
                      s.hrf_unit UnitOfMeasurment, \
                      first(l.address) Address,\
                      first(l.lat) Latitude,\
                      first(l.lon) Longitude,\
                      max(n.value_hrf) MaxTemperature, \
                      min(n.value_hrf) MinTemperature  \
                      FROM nodes n LEFT JOIN sensors s on  n.sensor=s.sensor\
                      LEFT JOIN locations l on n.node_id=l.node_id\
                      WHERE n.parameter = 'temperature' AND n.sensor = 'pr103j2'\
                      GROUP BY n.node_id, n.parameter, s.sensor, n.sensor, s.hrf_unit, timeDay \
                      ORDER BY n.node_id, timeDay"

queryForMaxMinConcPerDay = "SELECT DAY(n.timestamp) as timeDay, first(n.timestamp), n.node_id, n.parameter, count(*) NumberOfMeasurments, \
                      n.sensor NodesTableSensor, \
                      s.hrf_unit UnitOfMeasurment, \
                      first(l.address) Address,\
                      first(l.lat) Latitude,\
                      first(l.lon) Longitude,\
                      max(n.value_hrf) MaxConcentration, \
                      min(n.value_hrf) MinConcentration \
                      FROM nodes n LEFT JOIN sensors s on  n.sensor=s.sensor\
                      LEFT JOIN locations l on n.node_id=l.node_id\
                      WHERE n.parameter = 'concentration'\
                      GROUP BY n.node_id, n.parameter, s.sensor, n.sensor, s.hrf_unit, timeDay \
                      ORDER BY n.node_id, timeDay"

queryForMaxMinParticleCountPerDay = "SELECT DAY(n.timestamp) as timeDay, first(n.timestamp), n.node_id, n.parameter, count(*) NumberOfMeasurments, \
                      n.sensor NodesTableSensor, \
                      s.hrf_unit UnitOfMeasurment, \
                      first(l.address) Address,\
                      first(l.lat) Latitude,\
                      first(l.lon) Longitude,\
                      max(n.value_hrf) MaxParticleCount, \
                      min(n.value_hrf) MinParticleCount \
                      FROM nodes n LEFT JOIN sensors s on  n.sensor=s.sensor\
                      LEFT JOIN locations l on n.node_id=l.node_id\
                      WHERE s.hrf_unit = 'μg/m^3'\
                      GROUP BY n.node_id, n.parameter, s.sensor, n.sensor, s.hrf_unit, timeDay \
                      ORDER BY n.node_id, timeDay"     

# #--------------------------------------------------- Number of people - not working -----------------------------------------------------

# queryForNumberOfPeople = "SELECT n.node_id, n.parameter, count(*) NumberOfMeasurments, \
#                       n.sensor NodesTableSensor, \
#                       s.hrf_unit UnitOfMeasurment, \
#                       first(l.address) Address,\
#                       first(l.lat) Latitude,\
#                       first(l.lon) Longitude,\
#                       sum(n.value_hrf) NumberOfPeople \
#                       FROM nodes n LEFT JOIN sensors s on  n.sensor=s.sensor\
#                       LEFT JOIN locations l on n.node_id=l.node_id\
#                       WHERE n.parameter in ('car_total', 'person_total') AND n.sensor = 'image_detector' \
#                       GROUP BY n.node_id, n.parameter, s.sensor, n.sensor, s.hrf_unit \
#                       ORDER BY n.node_id"      

# ----------------------------------- SQL EXECUTION FOR AVERAGES PER MONTH PER NODE ------------------------------------
print("Calculating average temp per node per month...")
sqlDF = spark.sql(queryForAverageTemperature)
sqlDF.show()
sqlDF.coalesce(1).write.csv("hdfs://namenode:9000/results/averageTempPerNode.csv", header = 'true')
print("average temp over...")

print("Calculating average concentration per node per month...")
sqlAvgConc = spark.sql(queryForAverageConcentration)
sqlAvgConc.show()
sqlAvgConc.coalesce(1).write.csv("hdfs://namenode:9000/results/averageConcentrationPerNode.csv", header = 'true')
print("average concentration over...")

# ----------------------------------- SQL EXECUTION FOR AVERAGES PER HOUR PER NODE ------------------------------------

print("Calculating average temperature per hour per node...")
sqlAvgTempHour = spark.sql(queryForAverageTemperaturePerHour)
sqlAvgTempHour.show()
sqlAvgTempHour.coalesce(1).write.csv("hdfs://namenode:9000/results/averageTemperaturePerHourPerNode.csv", header = 'true')
print("average temperature per hour over...")

print("Calculating average concentration per hour per node...")
sqlAvgConcHour = spark.sql(queryForAverageConcentrationPerHour)
sqlAvgConcHour.show()
sqlAvgConcHour.coalesce(1).write.csv("hdfs://namenode:9000/results/averageConcentrationPerHourPerNode.csv", header = 'true')
print("average concentration per hour per node done...")

print("Calculating average particle count per hour per node...")
sqlAvgConcHour = spark.sql(queryForAverageParticleCountPerHour)
sqlAvgConcHour.show()
sqlAvgConcHour.coalesce(1).write.csv("hdfs://namenode:9000/results/averageParticleCountPerHourPerNode.csv", header = 'true')
print("average particle count per hour per node done...")

# ----------------------------------- SQL EXECUTION FOR MIN MAX PER DAY PER NODE ------------------------------------

print("Calculating min & max temperature per day per node...")
sqlMinMaxTempDay = spark.sql(queryForMaxMinTempPerDay)
sqlMinMaxTempDay.show()
sqlMinMaxTempDay.coalesce(1).write.csv("hdfs://namenode:9000/results/MinMaxPTemperaturePerDayPerNode.csv", header = 'true')
print("average min & max temperature per day per node done...")

print("Calculating min & max concentration per day per node...")
sqlMinMaxConcDay = spark.sql(queryForMaxMinConcPerDay)
sqlMinMaxConcDay.show()
sqlMinMaxConcDay.coalesce(1).write.csv("hdfs://namenode:9000/results/MinMaxConcentrationPerDayPerNode.csv", header = 'true')
print("average min & max concentration per day per node done...")

print("Calculating min & max particle count per day per node...")
sqlMinMaxParticleDay = spark.sql(queryForMaxMinParticleCountPerDay)
sqlMinMaxParticleDay.show()
sqlMinMaxParticleDay.coalesce(1).write.csv("hdfs://namenode:9000/results/MinMaxParticleCountPerDayPerNode.csv", header = 'true')
print("average min & max particle count per day per node done...")

# ----------------------------------- SQL EXECUTION FOR MIN MAX PER HOUR PER NODE ------------------------------------

print("Calculating min & max temperature per Hour per node...")
sqlMinMaxTempHour = spark.sql(queryForMaxMinTempPerHour)
sqlMinMaxTempHour.show()
sqlMinMaxTempHour.coalesce(1).write.csv("hdfs://namenode:9000/results/MinMaxPTemperaturePerHourPerNode.csv", header = 'true')
print("average min & max temperature per Hour per node done...")

print("Calculating min & max concentration per Hour per node...")
sqlMinMaxConcHour = spark.sql(queryForMaxMinConcPerHour)
sqlMinMaxConcHour.show()
sqlMinMaxConcHour.coalesce(1).write.csv("hdfs://namenode:9000/results/MinMaxConcentrationPerHourPerNode.csv", header = 'true')
print("average min & max concentration per Hour per node done...")

print("Calculating min & max particle count per day per node...")
sqlMinMaxParticleHour = spark.sql(queryForMaxMinParticleCountPerHour)
sqlMinMaxParticleHour.show()
sqlMinMaxParticleHour.coalesce(1).write.csv("hdfs://namenode:9000/results/MinMaxParticleCountPerHourPerNode.csv", header = 'true')
print("average min & max particle count per Hour per node done...")

# sqlPeople = spark.sql(queryForNumberOfPeople)
# sqlPeople.show()
# sqlPeople.coalesce(1).write.csv("hdfs://namenode:9000/results/numOfPeople.csv", header = 'true')
# print("average num of people over...")

# #--------------------------------------------------- STREAMING DATA CREATION  ----------------------------------------------------------
# queryForMakingStreamingData = "SELECT n.timestamp, n.node_id, n.sensor, n.parameter, n.value_hrf, s.hrf_unit, s.hrf_minval, s.hrf_maxval,\
#                       l.address Address,\
#                       l.lat Latitude,\
#                       l.lon Longitude\
#                       FROM nodes n LEFT JOIN sensors s on  n.sensor=s.sensor\
#                       LEFT JOIN locations l on n.node_id=l.node_id\
#                       WHERE n.parameter = 'concentration' \
#                       ORDER BY n.timestamp"
# queryForMakingStreamingData = "SELECT n.timestamp, n.node_id, n.sensor, n.parameter, n.value_hrf, s.hrf_unit, s.hrf_minval, s.hrf_maxval,\
#                       l.address Address,\
#                       l.lat Latitude,\
#                       l.lon Longitude\
#                       FROM nodes n LEFT JOIN sensors s on  n.sensor=s.sensor\
#                       LEFT JOIN locations l on n.node_id=l.node_id\
#                       WHERE s.hrf_unit = 'μg/m^3' \
#                       ORDER BY n.timestamp"
# queryForMakingStreamingData = "SELECT n.timestamp, n.node_id, n.sensor, n.parameter, n.value_hrf, s.hrf_unit, s.hrf_minval, s.hrf_maxval,\
#                       l.address Address,\
#                       l.lat Latitude,\
#                       l.lon Longitude\
#                       FROM nodes n LEFT JOIN sensors s on  n.sensor=s.sensor\
#                       LEFT JOIN locations l on n.node_id=l.node_id\
#                       WHERE n.parameter = 'temperature' AND n.sensor = 'pr103j2' \
#                       ORDER BY n.timestamp"
# queryForMakingBatchData = "SELECT *\
#                       FROM nodes n \
#                       WHERE n.timestamp > '2020-09-19 00:00:00'"      

# ----------------------------------- SQL EXECUTION FOR GENERATING STREAMING DATA ------------------------------------
# print("Starting GM3 generation...")
# sqlStreamingData = spark.sql(queryForMakingStreamingData)
# sqlStreamingData.show()
# sqlStreamingData.coalesce(1).write.csv("hdfs://namenode:9000/data/streamingDataTEMPER.csv", header = 'true')
# print("GM3 over...")

# print("Calculating min & max particle count per day per node...")
# sqlMinMaxParticleDay = spark.sql(queryForMakingBatchData)
# sqlMinMaxParticleDay.show()
# sqlMinMaxParticleDay.coalesce(1).write.csv("hdfs://namenode:9000/data/data2.csv", header = 'true')
# print("average min & max particle count per day per node done...")
