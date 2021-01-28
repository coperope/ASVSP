#!/usr/bin/python
### before spark-submit: export PYTHONIOENCODING=utf8

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

conf = SparkConf().setAppName("uni").setMaster("local")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

quiet_logs(spark)

schemaString = "timestamp node_id subsystem sensor parameter value_raw value_href"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

df = spark.read.csv("hdfs://namenode:9000/data/data.csv", header=True, mode="DROPMALFORMED", schema=schema)

df = df.withColumn("value_raw", df["value_raw"].cast(FloatType()))
df = df.withColumn("value_href", df["value_href"].cast(FloatType()))
df.printSchema()
df.createOrReplaceTempView("nodes")
# #------------------------------------------------- SENSORS SCHEMA ------------------------------------------------------------------
sensorsSchemaString = "ontology subsystem sensor parameter hrf_unit hrf_minval href_maxval datasheet"
sensorsFields = [StructField(field_name, StringType(), True) for field_name in sensorsSchemaString.split()]
sensorsSchema = StructType(sensorsFields)
dfSensors = spark.read.csv("hdfs://namenode:9000/data/sensors.csv", header=True, mode="DROPMALFORMED", schema=sensorsSchema)
dfSensors.createOrReplaceTempView("sensors")
dfSensors = dfSensors.withColumn("hrf_minval", dfSensors["hrf_minval"].cast(IntegerType()))
dfSensors = dfSensors.withColumn("href_maxval", dfSensors["href_maxval"].cast(IntegerType()))
# #------------------------------------------------- LOCATION SCHEMA ------------------------------------------------------------------
locationSchemaString = "node_id project_id vsn address lat lon start_timestamp end_timestamp"
locationFields = [StructField(field_name, StringType(), True) for field_name in locationSchemaString.split()]
locationSchema = StructType(locationFields)
dfLocation = spark.read.csv("hdfs://namenode:9000/data/nodes.csv", header=True, mode="DROPMALFORMED", schema=locationSchema)
dfLocation.createOrReplaceTempView("locations")
dfLocation = dfLocation.withColumn("lat", dfLocation["lat"].cast(FloatType()))
dfLocation = dfLocation.withColumn("lon", dfLocation["lon"].cast(FloatType()))
# #--------------------------------------------------- Queries ----------------------------------------------------------------------

# queryForDelayType = "SELECT Year, Month, count(*) NumberOfDelFlights, round(sum(ArrDelayMinutes)/count(*),1) AvgDelayInMin, \
#          round(sum(CarrierDelay)/count(*),1) AvgCarrierDelayInMin, \
#          round(sum(WeatherDelay)/count(*),1) AvgWeatherDelayInMin, \
#          round(sum(NASDelay)/count(*),1) AvgNASDelayInMinutes, \
#          round(sum(SecurityDelay)/count(*),1) AvgSecurityDelayInMin, \
#          round(sum(LateAircraftDelay)/count(*),1) AvgLateAircraftDelayInMin \
#          FROM flights \
#          WHERE ArrDel15 = '1.00' \
#          GROUP BY Year, Month \
#          ORDER BY Year, Month"

# queryForAverageTemperature = "SELECT n.node_id, n.parameter, count(*) NumberOfMeasurments, \
#                       n.sensor NodesTableSensor, \
#                       s.hrf_unit UnitOfMeasurment, \
#                       first(l.address) Address,\
#                       first(l.lat) Latitude,\
#                       first(l.lon) Longitude,\
#                       round(sum(n.value_href)/count(*),2) AverageTemperature \
#                       FROM nodes n LEFT JOIN sensors s on  n.sensor=s.sensor\
#                       LEFT JOIN locations l on n.node_id=l.node_id\
#                       WHERE n.parameter = 'temperature' AND n.sensor = 'pr103j2' \
#                       GROUP BY n.node_id, n.parameter, s.sensor, n.sensor, s.hrf_unit \
#                       ORDER BY n.node_id"

queryForNumberOfPeople = "SELECT n.node_id, n.parameter, count(*) NumberOfMeasurments, \
                      n.sensor NodesTableSensor, \
                      s.hrf_unit UnitOfMeasurment, \
                      first(l.address) Address,\
                      first(l.lat) Latitude,\
                      first(l.lon) Longitude,\
                      sum(n.value_href) NumberOfPeople \
                      FROM nodes n LEFT JOIN sensors s on  n.sensor=s.sensor\
                      LEFT JOIN locations l on n.node_id=l.node_id\
                      WHERE n.parameter in ('car_total', 'person_total') AND n.sensor = 'image_detector' \
                      GROUP BY n.node_id, n.parameter, s.sensor, n.sensor, s.hrf_unit \
                      ORDER BY n.node_id"
# queryForHourlyDelay = "SELECT DepTimeBlk HoursInterval, count(*) NumberOfFlights, \
#                       round(sum(ArrDel15)*100/count(*),1) DelayedMoreThan15MinPercentage, \
#                       round(sum(Cancelled)*100/count(*),1) CancelledPercentage, \
#                       round(count(Div1Airport)*100/count(*),1) DivertedPercentage \
#                       FROM flights \
#                       GROUP BY DepTimeBlk \
#                       ORDER BY DepTimeBlk"

# queryForOriginDelay = "SELECT OriginAirportId AirportId, OriginStateName StateName, OriginCityName CityName, Origin AirportName, count(*) NumberOfFlights, \
#                       round(sum(ArrDel15)*100/count(*),1) DelayedMoreThan15MinPercentage, \
#                       round(sum(Cancelled)*100/count(*),1) CancelledPercentage, \
#                       round(count(Div1Airport)*100/count(*),1) DivertedPercentage \
#                       FROM flights \
#                       GROUP BY OriginAirportId, OriginCityName, OriginStateName, Origin \
#                       ORDER BY OriginStateName, OriginCityName"


# queryDistinct = "SELECT count(DISTINCT OriginAirportId) FROM flights"

# queryForPlanes = "SELECT Manufacturer Manufacturer, Model Model, FLOOR(avg(p.Production_Year)) AverageProdYear, count(*) NumberOfFlights, \
#                   round(sum(ArrDel15)*100/count(*),1) DelayedMoreThan15MinPercentage, \
#                   round(sum(Cancelled)*100/count(*),1) CancelledPercentage, \
#                   round(count(Div1Airport)*100/count(*),1) DivertedPercentage \
#                   FROM flights f, planes p \
#                   WHERE f.Tail_Number = p.Tail_Number \
#                   GROUP BY Manufacturer, Model \
#                   ORDER BY Manufacturer, Model"
# sqlPercentage = spark.sql(queryForMonthlyDelay)
# sqlPercentage.show(20,False)
# #sqlPercentage.coalesce(1).write.csv("hdfs://namenode:9000/results/monthlyDelay.csv", header = 'true')
# #print("Monthly over...")

# print("Calculating average temp per node...")
# sqlDF = spark.sql(queryForAverageTemperature)
# sqlDF.show()
# sqlDF.coalesce(1).write.csv("hdfs://namenode:9000/results/averageTempPerNode.csv", header = 'true')
# print("average temp over...")

sqlPeople = spark.sql(queryForNumberOfPeople)
sqlPeople.show()
sqlPeople.coalesce(1).write.csv("hdfs://namenode:9000/results/numOfPeople.csv", header = 'true')
print("average num of people over...")

# sqlOrigin = spark.sql(queryForOriginDelay)
# sqlOrigin.show(6000,20, False)
# #sqlOrigin.coalesce(1).write.csv("hdfs://namenode:9000/results/originDelay.csv", header = 'true')
# #print("Origin over...")

# #sqlDistinct = spark.sql(queryDistinct)
# #sqlDistinct.show()
# sqlPlanes = spark.sql(queryForPlanes)
# sqlPlanes.show(1000, False)
# #sqlPlanes.coalesce(1).write.csv("hdfs://namenode:9000/results/planesDelay.csv", header = 'true')
# #print("Plane over...")