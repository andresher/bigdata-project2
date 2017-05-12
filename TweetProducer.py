# Produce Tweets into Kafka

# Imports from Kafka and Twitter
from kafka import KafkaProducer
from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream
import json
import sys


def read_credentials():
    file_name = "credentials.json"
    try:
        with open(file_name) as data_file:
            return json.load(data_file)
    except:
        print ("Cannot load credentials.json")
        return None


def produce_tweets(access_token, access_secret, consumer_key, consumer_secret, host, topic):

    oauth = OAuth(access_token, access_secret, consumer_key, consumer_secret)

    # Initiate the connection to Twitter Streaming API
    twitter_stream = TwitterStream(auth=oauth)

    # Get a sample of the public data following through Twitter
    iterator = twitter_stream.statuses.sample()

    # Initiate Kafka Producer
    producer = KafkaProducer(bootstrap_servers=host)

    # Print each tweet in the stream to the screen
    # Here we set it to stop after getting 100 tweets.
    # You don't have to set it to stop, but can continue running
    # the Twitter API to collect data for days or even longer.
    tweet_count = 100
    for tweet in iterator:
        tweet_count -= 1
        # Twitter Python Tool wraps the data returned by Twitter
        # as a TwitterDictResponse object.
        try:
            # print screen_name and name
            print("TWEET: ", tweet['user']['screen_name'])
            # The command below will do pretty printing for JSON data, try it out
            print("TWEET JSON: ", json.dumps(tweet, indent=4))
            # This next command, prints the tweet as a string
            print ("TWEETS STRING", str(tweet))
            # test
            print("TWEET BYTE: ", str.encode(tweet))
            # This will send the tweet to the specified topic
            producer.send(topic, str.encode(tweet))
        except:
            pass

        if tweet_count <= 0:
            print("Done")
            break

if __name__ == "__main__":
    print('Number of arguments: ', len(sys.argv), 'arguments.')
    print('Argument List: ', str(sys.argv))
    if len(sys.argv) != 5:
        print("Usage: spark-submit ./TweetProducer.py --master <master> <host:port> <topic>", file=sys.stderr)
        exit(-1)

    host = sys.argv[3]
    topic = sys.argv[4]

    print("host: ", host)
    print("topic: ", topic)

    print("Stating to produce tweets")
    credentials = read_credentials()
    produce_tweets(credentials['ACCESS_TOKEN'], credentials['ACCESS_SECRET'],
                credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'], host, topic)