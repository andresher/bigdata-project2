from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import SimpleProducer, KafkaClient
from kafka import KafkaProducer
from operator import add
from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream
from pyspark.sql import Row, SparkSession
from stop_words import get_stop_words
try:
    import json
except ImportError:
    import simplejson as json
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars $SPARK_HOME/jars/spark-streaming-kafka-0-8-assembly_2.11.jar pyspark-shell'

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession.builder.config("spark.sql.warehouse.dir", '/user/hive/warehouse').enableHiveSupport().getOrCreate()
    return globals()['sparkSessionSingletonInstance']

def consumer():
    context = StreamingContext(sc, 30)
    dStream = KafkaUtils.createDirectStream(context, ["trump"], {"metadata.broker.list": "localhost:9092"})
    dStream.foreachRDD(p1)
    context.start()
    context.awaitTermination()

# Part 4.a
def insertHashtags(hashtags, spark, time):
    if hastags:
        rddHashtags = sc.parallelize(hashtags)
        rddHashtags = rddHashtags.map(lambda x: x.lower())
        if rddHashtags.count() > 0:
            # Convert RDD[String] to RDD[Row] to DataFrame
            hashtagsDataFrame = spark.createDataFrame(rddHashtags.map(lambda x: Row(hashtag=x, timestamp=time)))
            hashtagsDataFrame.createOrReplaceTempView("hashtags")
            hashtagsDataFrame = spark.sql("create database if not exists bdp2")
            hashtagsDataFrame = spark.sql("use bdp2")
            hashtagsDataFrame = spark.sql("select hastag, timestamp from hashtags")
            hashtagsDataFrame.write.mode("append").saveAsTable("hashtags")
            print("Inserted hashtags")
    else:
        print("No hashtags avaliable to insert in hive")

# Part 4.b
def insertText(text, spark, time):
    if text:
        stop_words = get_stop_words('en')
        rddText = sc.parallelize(text)
        rddText = rddText.flatMap(lambda x: x.split()).map(lambda x: x.lower())
        rddText = rddText.filter(lambda x: x not in stop_words)
        if rddText.count() > 0:
            # Convert RDD[String] to RDD[Row] to DataFrame
            textDataFrame = spark.createDataFrame(rddText.map(lambda x: Row(text=x, timestamp=time)))
            textDataFrame.createOrReplaceTempView("text")
            textDataFrame = spark.sql("create database if not exists bdp2")
            textDataFrame = spark.sql("use bdp2")
            textDataFrame = spark.sql("select text, timestamp from text")
            textDataFrame.write.mode("append").saveAsTable("text")
            print("Inserted text")
    else:
        print("No text avaliable to insert into hive")

def p1(time,rdd):
    rdd = rdd.map(lambda x: json.loads(x[1]))
    records = rdd.collect() #Return a list with tweets
    spark = getSparkSessionInstance(rdd.context.getConf())

    # Part 4.a
    hashtags = [element["entities"]["hashtags"] for element in records if "entities" in element]
    hashtags = [x for x in hashtags if x]
    hashtags = [element[0]["text"] for element in hashtags]
    insertHashtags(hashtags, spark, time)

    # Part 4.b
    text = [element["text"] for element in records if "text" in element]
    insertText(text, spark, time)

if __name__ == "__main__":
    print("Starting to read tweets")
    sc = SparkContext(appName="ConsumerTRUMP")
    consumer()
