from kafka import KafkaConsumer, KafkaProducer
import json
import os
from engine_utils import get_sensors
from engine_parser import parse_event
from engine_enrich import enrich_event
import logging
from engine_alerting import engine_alerting
from elasticsearch import Elasticsearch, helpers

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

es = Elasticsearch([os.environ.get("ES_HOST", "http://localhost:9200")])
def process_event_realtime(enriched_event, tenant):
    """
    Process and write a single enriched event to Elasticsearch in real-time.

    :param enriched_event: The enriched event to process.
    :param tenant_id: The tenant's ID for index naming.
    """
    # Define the tenant-specific index name
    tenant_index = f"{tenant}-events"  # Changed to avoid wildcard

    # Prepare the event data for Elasticsearch
    event_data = {
        "_index": tenant_index,  # Index for the tenant
        "_source": enriched_event  # Event data
    }

    # Write the event to Elasticsearch using the bulk API for efficiency
    try:
        helpers.bulk(es, [event_data])  # Using a list with a single item
        print(f"Successfully indexed event to {tenant_index}")
    except Exception as e:
        print(f"Failed to index event to Elasticsearch: {e}")
        
def process_batch(events, producer):
    '''Process a batch of Kafka events.'''
    for event in events:
        try:
            tenant = event.get("tenant")
            parsed_event = parse_event(event)
            if parsed_event and parsed_event.get("is_parsed") == 1:
                enriched_event = enrich_event(parsed_event)
                process_event_realtime(enriched_event,tenant)
                # Send the enriched event to the DB event topic
                # enriched_event_dump = json.dumps(enriched_event)
                # tenant_db_topic = f"{tenant}-db-events"
                # print(enriched_event)
                # try:
                #     producer.send(tenant_db_topic, value=enriched_event_dump)
                #     producer.flush()
                #     logger.info(f"Enriched event sent to DB topic: {tenant_db_topic}")
                # except Exception as e:
                #     print("Error while sending message------->" , e)
                
                
                # Check for alerts and handle them
                alert = engine_alerting(enriched_event)
                if alert.get("is_alert"):
                    logger.info(f"Alert triggered for event: {alert}")
                    # Handle the alert logic (e.g., send an alert notification)

        except Exception as e:
            logger.error(f"Failed to process event {event}: {e}")

def main():
    '''Entry point for consuming Kafka events and processing them'''

    # Kafka configuration
    kafka_brokers = os.environ.get("KAFKA_BROKERS", "127.0.0.1:9092")
    kafka_topics = os.environ.get("KAFKA_TOPICS", "dev-events-topic,dev2-events-topic").split(",")

    # Initialize Kafka consumer
    consumer = KafkaConsumer(
        *kafka_topics,
        bootstrap_servers=kafka_brokers,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id='threat-detection-group',
        auto_offset_reset='earliest',
        max_poll_records=100,
        fetch_max_bytes=1048576,
        max_partition_fetch_bytes=1048576,
        enable_auto_commit=False  # Disable auto-commit to control commit manually
    )

    # Initialize Kafka producer -- sending to db-writer-topic for elastic search and db(if needed)
    producer = KafkaProducer(
            bootstrap_servers=kafka_brokers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: json.dumps(x).encode('utf-8'),
            retries=5,  # Retry mechanism in case of failure
            batch_size=16384,  # Increased batch size for Kafka producer
            compression_type='gzip'  # Compress messages to reduce network load
        )

    try:
        while True:
            events = consumer.poll(timeout_ms=500)  # Poll for new events with timeout
            if events:
                # Flatten the fetched messages
                batch = [message.value for partition in events.values() for message in partition]
                
                if batch:
                    logger.info(f"Processing batch of {len(batch)} events")
                    process_batch(batch, producer)
                    
                    # Manually commit after successful processing
                    consumer.commit()
                    logger.info("Batch successfully committed")

    except Exception as e:
        logger.error(f"Error during Kafka consumption: {e}")

    finally:
        consumer.close()
        producer.close()
        logger.info("Kafka consumer and producer closed.")

if __name__ == "__main__":
    main()
