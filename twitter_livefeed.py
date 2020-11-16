# Importing libraries 
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaConsumer, KafkaProducer
import json
#Twittwe API credentials
access_token = "378067031-g4SQ2mqogTpYpWGfI8PRuWio8wvLAINXA1ML9T"   
access_token_secret =  "q86ma7UolOdKlSx0dLC6aS5F0ASJ1giLjcYHPERCasP"
consumer_key =  "18v3tTbDLW6Tb4E9gJraP5G"
consumer_secret =  "95sijBmumpQpHczNKBCaBXs0WDzVnLqyunPyM519zzLZZvr"
class StdOutListener(StreamListener):
    def on_data(self, data):
        json_data = {}
        json_data = json.loads(data)
        #Send twitter text to kafka topic "twitter_topic"
        if 'text' in json_data:
            print (json_data["text"])
            producer.send("twitter_demo", json_data["text"].encode('utf-8'))
        else:
            print("text not found")
        return True
    def on_error(self, status):
        print (status)
#kafka installed in localhost
KAFKA_HOSTS = ['localhost:9092']
KAFKA_VERSION = (0, 10)
producer= KafkaProducer(bootstrap_servers=KAFKA_HOSTS, api_version=KAFKA_VERSION)
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
#Filter tweets which have the word climate
stream.filter(track=["covid19"])
