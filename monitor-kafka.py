import time
from kafka import KafkaConsumer


topic_name = ["raw-tweets", "en-tweets", "fr-tweets", "positive-tweets", "negative-tweets"]
consumer = KafkaConsumer(bootstrap_servers="localhost:9092", group_id="Monitor")

#print(consumer.topics())
consumer.subscribe(topics=topic_name)
print("the List of subscribed Topics is ",consumer.subscription())


for message in consumer:
    print(" Topic-Name : {}, Partition-id: {}, Offset-id: {}, timestamp: {}".format(
    message.topic, message.partition, message.offset, message.timestamp))
    time.sleep(0.3)
