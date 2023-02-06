from kafka import KafkaProducer
from kafka import KafkaConsumer
from json import loads
from json import dumps

#Producer defined to filter tweets 
producer_filter = KafkaProducer(bootstrap_servers=["localhost:9092"],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))


#Consumer to read tweets from raw-tweets topic
consumer = KafkaConsumer(
      "raw-tweets",
      bootstrap_servers=["localhost:9092"],auto_offset_reset="earliest",
      value_deserializer=lambda x: x.decode('utf-8'))

print(producer_filter.bootstrap_connected())
print(consumer.bootstrap_connected())

i=0
for message in consumer:
    tweet = loads(message.value)
    lang = str(tweet['lang']) # language of the tweet 
    i=i+1
    if lang.upper() == "FR" :
        producer_filter.send("fr-tweets", tweet) # send the tweet
    elif lang.upper() == "EN":
        producer_filter.send("en-tweets", tweet) # send the tweet
        
