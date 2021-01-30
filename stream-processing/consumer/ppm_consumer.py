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
        print("Usage: main_consumer.py <zk> <topic>", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="SparkStreamingKafkaChicagoSensors")
    quiet_logs(sc)

    ssc = StreamingContext(sc, 10)
    # ssc.checkpoint("stateful_checkpoint_direcory")
    thresholds = {
        "co": -1.0,
        "o3": 0.002,
        "h2s": 0,
        "no2": 0.04,
        "so2": 0.09,
        "oxidizing_gases": 0.5,
        "reducing_gases": -0.03 
    }
    zoo, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zoo, "spark-streaming-consumer", {topic: 1})
    # parsed = kvs.map(lambda v: json.loads(v[1]))
    lines = kvs \
        .map(lambda x: "{0} {1}".format(x[0], x[1])) \
        .map(lambda line: "{0} {1} {2} {3} {4} {5}".format(line.split()[0],
                                                       line.split()[1],
                                                       line.split()[11] + line.split()[12] + line.split()[13],
                                                       line.split()[6],
                                                       "**************** ERROR - sensor output out of range ***************" if float(
                                                           line.split()[7].replace(",", "").replace("\"", "")) < float(
                                                           line.split()[9].replace(",", "").replace("\"", "").replace(
                                                               "\"", "")) - 1.0 or float(
                                                           line.split()[7].replace(",", "").replace("\"", "")) > float(
                                                           line.split()[10].replace(",", "").replace("\"", "")) else
                                                       " ",
                                                       "---------- Warning - particle concentration is very high: " + line.split()[7] + " ppm" if float(line.split()[7].replace(",", "").replace("\"", "")) > thresholds[line.split()[5].replace(",", "").replace("\"", "")] else line.split()[7] + " ppm" ))

    lines.pprint(9)

    ssc.start()
    ssc.awaitTermination()