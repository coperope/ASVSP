r"""
 Run the example
    `$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 $SPARK_HOME/primeri/kafka_wordcount.py zoo:2181 subreddit-politics`
    `$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 $SPARK_HOME/primeri/kafka_wordcount.py zoo:2181 subreddit-politics subredit-funny`
"""
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import os
import math

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]


def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="SparkStreamingKafkaChicagoSensors")
    quiet_logs(sc)

    ssc = StreamingContext(sc, 3)

    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    lines = kvs \
        .map(lambda x: "{0} {1}".format(x[0], x[1])) \
        .map(lambda line: "{0} {1} {2} {3} {4} {5}".format(line.split()[0],
            line.split()[1],
            line.split()[2],
            line.split()[3],
            round(float(line.split()[10]) + (float(line.split()[11]) * (math.cos(float(line.split()[12]) * math.pi / 180))), 3),
            "warning" if float(line.split()[10]) + (float(line.split()[11]) * (math.cos(float(line.split()[12]) * math.pi / 180))) > 800 else ""))

    lines.pprint()

    ssc.start()
    ssc.awaitTermination()