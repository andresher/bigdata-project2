from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import SimpleProducer, KafkaClient
from kafka import KafkaProducer
from operator import add
from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream
from pyspark.sql import Row, SparkSession
try:
    import json
except ImportError:
    import simplejson as json
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars $SPARK_HOME/jars/spark-streaming-kafka-0-8-assembly_2.11.jar pyspark-shell'

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession.builder.config(conf=sparkConf).enableHiveSupport().getOrCreate()
    return globals()['sparkSessionSingletonInstance']

def consumer():
    context = StreamingContext(sc, 30)
    dStream = KafkaUtils.createDirectStream(context, ["test"], {"metadata.broker.list": "localhost:9092"})
    dStream.foreachRDD(p1)
    context.start()
    context.awaitTermination()

def p1(time,rdd):
    rdd = rdd.map(lambda x: json.loads(x[1]))
    records = rdd.collect() #Return a list with tweets
    spark = getSparkSessionInstance(rdd.context.getConf())

    keywords = [element["text"] for element in records if "text" in element]
    insertText(keywords, spark, time)

def insertText(keywords, spark, time):
    if keywords:
        rddKeywords = sc.parallelize(keywords)
        rddKeywords = rddKeywords.map(lambda x: x.lower()).filter(lambda x: "trump" in x)
        if rddKeywords.count() > 0:
            # Convert RDD[String] to RDD[Row] to DataFrame
            keywordsDataFrame = spark.createDataFrame(rddKeywords.map(lambda x: Row(tweet=x, hour=time)))
            keywordsDataFrame.createOrReplaceTempView("fastcapture")
            keywordsDataFrame = spark.sql("use bdp2")
            keywordsDataFrame = spark.sql("select tweet, hour from fastcapture")
            keywordsDataFrame.write.mode("append").saveAsTable("fastcapture")
            print("Inserted fastcapture FINISH")
    else:
        print("No keywords avaliable to insert in hive")

if __name__ == "__main__":
    print("Starting to read tweets")
    sc = SparkContext(appName="ConsumerTRUMP")
    consumer()
