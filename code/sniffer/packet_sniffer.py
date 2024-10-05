import socket
import os
import kafka
from kafka import KafkaProducer
import subprocess
import json
import logging
import select

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def set_promiscuous_mode(interface):
    '''Set the network interface to promiscuous mode.'''
    try:
        subprocess.check_call(['ip', 'link', 'set', interface, 'promisc', 'on'], shell=True)
        logger.info(f"Promiscuous mode enabled on {interface}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to set promiscuous mode: {e}")

def create_kafka_producer(broker):
    '''Create and return a Kafka producer.'''
    try:
        producer = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: json.dumps(x).encode('utf-8'),
            retries=5,  # Retry mechanism in case of failure
            batch_size=16384,  # Increased batch size for Kafka producer
            linger_ms=50,  # Send messages after 50ms to allow more batching
            compression_type='gzip'  # Compress messages to reduce network load
        )
        logger.info(f"Kafka producer connected to broker: {broker}")
        return producer
    except kafka.errors.NoBrokersAvailable as e:
        logger.error(f"No Kafka brokers available: {e}")
        raise

def send_batch_to_kafka(producer, topic, batch_data):
    '''Send a batch of packet data to Kafka topic.'''
    try:
        for data in batch_data:
            producer.send(topic, value=data)
        producer.flush()
        logger.info(f"Sent batch of {len(batch_data)} packets to topic {topic}")
    except Exception as e:
        logger.error(f"Failed to send batch to Kafka: {e}")

def main_runner(interface):
    # Set the network interface to promiscuous mode
    set_promiscuous_mode(interface)

    # Create a raw socket to capture packets
    conn = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.ntohs(3))
    conn.bind((interface, 0))

    # Kafka setupgit
    kafka_broker = os.environ.get("KAFKA_BROKER", "127.0.0.1:9092")
    kafka_topic = os.environ.get("KAFKA_TOPIC", "dev-events-topic")  # tenant-specific topic
    tenant_id = kafka_topic.split("-")[0]  # Extract tenant ID from topic

    # Create Kafka producer
    producer = create_kafka_producer(kafka_broker)

    packet_batch = []  # List to hold packets for batching
    batch_size = 100  # Increased batch size to improve performance
    timeout = 5  # Send batch every 5 seconds if not full

    try:
        while True:
            ready_sockets, _, _ = select.select([conn], [], [], timeout)
            if ready_sockets:
                raw_data, addr = conn.recvfrom(65536)

                # Prepare the packet data
                raw_pkt = {
                    "tenant": tenant_id,
                    "raw_data": raw_data.hex(),  # Convert raw packet to hex string
                    "source_type": "packet",
                    "interface": interface
                }
                print(raw_pkt)

                # Add packet to batch
                packet_batch.append(raw_pkt)

            # Send batch when it reaches the batch size or after timeout
            if len(packet_batch) >= batch_size or not ready_sockets:
                send_batch_to_kafka(producer, kafka_topic, packet_batch)
                packet_batch = []  # Clear the batch after sending

    except KeyboardInterrupt:
        logger.info("Packet capturing stopped.")
        if packet_batch:
            logger.info(f"Sending remaining {len(packet_batch)} packets before shutdown...")
            send_batch_to_kafka(producer, kafka_topic, packet_batch)
        producer.close()
        logger.info("Kafka producer closed.")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        producer.close()

if __name__ == "__main__":
    # Specify the network interface (e.g., eth0, wlan0, etc.)
    network_interface = os.environ.get("NETWORK_INTERFACE", 'eth0')
    main_runner(network_interface)