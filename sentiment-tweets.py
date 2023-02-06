from kafka import KafkaProducer
from kafka import KafkaConsumer
from json import loads
from json import dumps

#Producer defined to classify tweets 
producer_classify = KafkaProducer(bootstrap_servers=["localhost:9092"],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))


consumer = KafkaConsumer(bootstrap_servers=["localhost:9092"],auto_offset_reset="earliest",
      value_deserializer=lambda x: x.decode('utf-8'))

consumer.subscribe(topics=["fr-tweets","en-tweets"])
print("the List of subscribed Topics is ",consumer.subscription())

print(producer_classify.bootstrap_connected())
print(consumer.bootstrap_connected())

i=0      
for message in consumer:
    tweet = loads(message.value)
    sentiment = str(tweet['possibly_sensitive']) # language of the tweet
    i=i+1
    if sentiment.upper() == "FALSE" :
        producer_classify.send("negative-tweets", tweet) # send the tweet
    if sentiment.upper() == "TRUE" :
        producer_classify.send("positive-tweets", tweet) # send the tweet
