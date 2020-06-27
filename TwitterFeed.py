#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import tweepy
import time
from kafka import KafkaConsumer, KafkaProducer

# twitter setup
access_token = "378067031-g4SQ2mqogTpYpWGfI8PRuWio8wvLAINXA1ML9Tws"
access_token_secret =  "q86ma7UolOdKlSx0dLC6aS5F0ASJ1giLjcYHPEQwRCasP"
consumer_key =  "18v3tTbDLW6Tb4E9gJraPqD5G"
consumer_secret =  "95sijBmumpQpHczLLwNKBCaBXs0WDzVnLqyunPyM519zzLZZvr"
# Creating the authentication object
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
# Setting your access token and secret
auth.set_access_token(access_token, access_token_secret)
# Creating the API object by passing in auth information
api = tweepy.API(auth)


from datetime import datetime, timedelta

def normalize_timestamp(time):
    mytime = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
    mytime += timedelta(hours=1)   # the tweets are timestamped in GMT timezone, while I am in +1 timezone
    return (mytime.strftime("%Y-%m-%d %H:%M:%S"))
KAFKA_HOSTS = ['localhost:9092']
KAFKA_VERSION = (0, 10)
producer = KafkaProducer(bootstrap_servers=KAFKA_HOSTS, api_version=KAFKA_VERSION)

topic_name = "TopicTestTalend"

#producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'],
#api_version=(0,11,5),
#value_serializer=lambda x: dumps(x).encode('utf-8'))
#topic_name = 'TopicTestTalend'

def get_twitter_data():
    res = api.search("covid19")
    for i in res:
        record = ''
        record += str(i.user.id_str)
        record += ';'
        record += str(normalize_timestamp(str(i.created_at)))
        record += ';'
        record += str(i.user.followers_count)
        record += ';'
        record += str(i.user.location)
        record += ';'
        record += str(i.favorite_count)
        record += ';'
        record += str(i.retweet_count)
        record += ';'
        producer.send(topic_name, str.encode(record))


get_twitter_data()

def periodic_work(interval):
    while True:
        get_twitter_data()
        #interval should be an integer, the number of seconds to wait
        time.sleep(interval)

periodic_work(60 * 0.1)  # get data every couple of minutes


# In[4]:


from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaConsumer, KafkaProducer
import json
access_token = "378067031-g4SQ2mqogTpYpWGfI8PRuWio8wvLAINXA1ML9Tws"
access_token_secret =  "q86ma7UolOdKlSx0dLC6aS5F0ASJ1giLjcYHPEQwRCasP"
consumer_key =  "18v3tTbDLW6Tb4E9gJraPqD5G"
consumer_secret =  "95sijBmumpQpHczLLwNKBCaBXs0WDzVnLqyunPyM519zzLZZvr"
class StdOutListener(StreamListener):
    def on_data(self, data):
        json_data = {}
        json_data = json.loads(data)
        #Send twitter text to kafka topic "twitter_topic"
        if 'text' in json_data:
            print (json_data["text"])
            producer.send("TopicTestTalend", json_data["text"].encode('utf-8'))
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
stream.filter(track=["india"])


# In[ ]:


import tweepy
access_token = "378067031-g4SQ2mqogTpYpWGfI8PRuWio8wvLAINXA1ML9Tws"
access_token_secret =  "q86ma7UolOdKlSx0dLC6aS5F0ASJ1giLjcYHPEQwRCasP"
consumer_key =  "18v3tTbDLW6Tb4E9gJraPqD5G"
consumer_secret =  "95sijBmumpQpHczLLwNKBCaBXs0WDzVnLqyunPyM519zzLZZvr"

# Creating the authentication object
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
# Setting your access token and secret
auth.set_access_token(access_token, access_token_secret)
# Creating the API object while passing in auth information
api = tweepy.API(auth) 

# Using the API object to get tweets from your timeline, and storing it in a variable called public_tweets
public_tweets = api.home_timeline()
# foreach through all tweets pulled
for tweet in public_tweets:
   # printing the text stored inside the tweet object
   print(tweet.text)


# In[ ]:


print(tweet.created_at)


# In[ ]:


# Creating the API object while passing in auth information
api = tweepy.API(auth)

# The search term you want to find
query = "India"
# Language code (follows ISO 639-1 standards)
language = "en"

# Calling the user_timeline function with our parameters
results = api.search(q=query, lang=language)

# foreach through all tweets pulled
for tweet in results:
   # printing the text stored inside the tweet object
   print(tweet.user.screen_name,"Tweeted:",tweet.text)

