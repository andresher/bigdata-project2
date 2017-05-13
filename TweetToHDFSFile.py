from kafka import KafkaConsumer
from hdfs import InsecureClient
from datetime import datetime
from datetime import timedelta
from json import dumps


client = InsecureClient('http://136.145.217.169:9000', user='andres.hernandez2')
consumer = KafkaConsumer('trump')
directoryPath = '/home/andres/project3'

while True:
    now = datetime.now()
    nowS = now.strftime('%Y-%m-%d-%H:%M:%S')

    for tweet in consumer:

        with client.write(directoryPath + '/' + nowS + '.txt') as writer:
            writer.write(tweet)

        tdelta = datetime.now() - now
        if tdelta.total_seconds() >= 600:
            break

