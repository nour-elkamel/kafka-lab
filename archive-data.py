from kafka import KafkaConsumer
from json import loads


consumer = KafkaConsumer(
      bootstrap_servers=["localhost:9092"],
      group_id = "archive_activity",
      value_deserializer=lambda x: x.decode('utf-8'))

consumer.subscribe(topics=["raw-tweets", "en-tweets", "fr-tweets", "positive-tweets", "negative-tweets"])

file = open("archive_data.txt", "a", encoding="utf-8")

Max_records = 99999  # Max records to be saved to the file
counter = 0

for message in consumer : 
    tweet = loads(message.value)
    file.write(str(tweet)+"\r\n") # write station details to the file
    counter += 1
    if (counter >= Max_records):
        break
         

file.close()
