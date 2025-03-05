import os
from kafka import KafkaConsumer

topic = 'movielog1'

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='team01',
    auto_commit_interval_ms=1000
)

print('Reading Kafka Broker')
for message in consumer:
    message = message.value.decode()
    os.system(f"echo {message} >> data/movie_logs.csv")
