from kafka import KafkaConsumer, TopicPartition

### This script is used to read messages from a Kafka topic in batches and process them.

# Configuration
bootstrap_servers = 'localhost:9092'  # Replace with your Kafka broker(s)
topic_name = 'movielog1'             # Replace with your topic name
partition = 0                         # Partition to read from (use 0 or specify the desired partition)
start_offset = 0                      # Starting offset for old data
batch_size = 1000                       # Number of messages to process in each batch

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
        # group_id='team01',
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

def get_partition_info(topic_name, partition_number):
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id='team01'
    )
    
    tp = TopicPartition(topic_name, partition_number)
    consumer.assign([tp])
    
    # Get beginning and end offsets
    beginning_offset = consumer.beginning_offsets([tp])[tp]
    end_offset = consumer.end_offsets([tp])[tp]
    
    # Calculate number of messages
    num_messages = end_offset - beginning_offset
    
    print(f"Partition {partition_number}:")
    print(f"- Beginning offset: {beginning_offset}")
    print(f"- End offset: {end_offset}")
    print(f"- Number of messages: {num_messages}")
    
    consumer.close()
    return num_messages

# Check all partitions
def get_topic_info(topic_name):
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id='team01'
    )
    
    # Get all partitions for the topic
    partitions = consumer.partitions_for_topic(topic_name)
    total_messages = 0
    
    print(f"\nTopic: {topic_name}")
    print("-" * 30)
    
    for partition in partitions:
        messages = get_partition_info(topic_name, partition)
        total_messages += messages
    
    print("-" * 30)
    print(f"Total messages across all partitions: {total_messages}")
    consumer.close()


if __name__ == "__main__":
    get_topic_info(topic_name)
    # main()