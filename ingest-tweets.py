import tweepy
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time

def ingest_tweets (query, bearer_token):
    client = tweepy.Client(bearer_token=bearer_token)
    # Replace with your own search query
    query = query
    tweets = []
    # Replace the limit=1000 with the maximum number of Tweets you want
    for tweet in tweepy.Paginator(client.search_recent_tweets, query=query,
      tweet_fields=[ 'created_at', 'lang', 'possibly_sensitive'], max_results=100).flatten(limit=1000):
      #print(tweet, tweet['lang'], tweet['possibly_sensitive'])
      #print(tweet['data'])
      print({'tweet_id':tweet['id'], 'tweet_text': tweet['text'], 'tweet_date': str(tweet['created_at']),  'lang':tweet['lang'], 'possibly_sensitive': tweet['possibly_sensitive']})
      tweets.append({'tweet_id':tweet['id'], 'tweet_text': tweet['text'], 'tweet_date': str(tweet['created_at']),  'lang':tweet['lang'], 'possibly_sensitive': tweet['possibly_sensitive']})
      print('\n')
    return tweets


tweets = ingest_tweets('covid -is:retweet', 'AAAAAAAAAAAAAAAAAAAAACFXkAEAAAAAbtJUYE49rNDIFIapK%2Fb8QkIRvVE%3DGLbLOuHuiHv0he5a1Gi4XhOb00ieRziAkcgbl5HrAprJRf8c9Y')

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
              api_version=(0,18,3),
              value_serializer=lambda x: x.encode('utf-8'))
i =0
topic_name = "tweets"
for tweet in tweets:
    message = "message-{}".format(i)
    data = {'n': i}
    producer.send(topic_name, json.dumps(tweet))
    print("Sending message {} to topic: {}".format(message, topic_name))
    i += 1
    time.sleep(1)
