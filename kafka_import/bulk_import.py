### This script is used to read messages from a Kafka topic in batches and process them.

from kafka import KafkaConsumer, TopicPartition

# Configuration
bootstrap_servers = 'localhost:9092'  # Replace with your Kafka broker(s)
topic_name = 'your_topic'             # Replace with your topic name
partition = 0                         # Partition to read from (use 0 or specify the desired partition)
start_offset = 0                      # Starting offset for old data
batch_size = 10                       # Number of messages to process in each batch

def process_batch(messages):
    """
    Function to process a batch of messages.
    Replace this with your custom processing logic.
    """
    print("Processing batch:")
    for message in messages:
        print(f"Offset: {message.offset}, Key: {message.key}, Value: {message.value.decode('utf-8')}")
    print("-" * 50)

def main():
    # Create a Kafka consumer
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        enable_auto_commit=False,  # Disable auto-commit to control offsets manually
        auto_offset_reset='earliest'  # Start from the earliest offset if no committed offset exists
    )

    # Assign the consumer to a specific partition and seek to the desired offset
    tp = TopicPartition(topic_name, partition)
    consumer.assign([tp])
    consumer.seek(tp, start_offset)

    try:
        while True:
            # Poll for messages (timeout in milliseconds)
            messages = consumer.poll(timeout_ms=1000, max_records=batch_size)

            if not messages:
                print("No more messages to consume.")
                break

            for tp, records in messages.items():
                process_batch(records)  # Process each batch of messages

            # Commit offsets manually after processing (optional)
            consumer.commit()
    
    except KeyboardInterrupt:
        print("Consumption interrupted by user.")
    
    finally:
        consumer.close()
        print("Kafka consumer closed.")

if __name__ == "__main__":
    main()
