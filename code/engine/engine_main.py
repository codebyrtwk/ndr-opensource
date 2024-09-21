from kafka import KafkaConsumer
import json
import os
from engine_utils import get_sensors
from engine_parser import parse_event
from engine_enrich import enrich_event
import logging
from engine_alerting import engine_alerting

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_batch(events):
    '''Process a batch of Kafka events.'''
    for event in events:
        try:
            tenant = event.get("tenant")
            parsed_event = parse_event(event)

            if parsed_event and parsed_event.get("is_parsed") == 1:
                enriched_event = enrich_event(parsed_event)
                logger.info(f"Processed event: {enriched_event}")
                # Perform further actions like inserting into a database
                # Example: insert_into_es(enriched_event)
                
            if enriched_event:
                alert = engine_alerting(enriched_event)
                if alert.get("is_alert"):
                    pass

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
        max_poll_records=100,  # Increased to fetch more records at once
        fetch_max_bytes=1048576,
        max_partition_fetch_bytes=1048576,
        enable_auto_commit=False  # Disable auto-commit to control commit manually
    )

    # Process events in batches
    try:
        while True:
            events = consumer.poll(timeout_ms=500)  # Poll for new events with timeout
            if events:
                # Flatten the fetched messages
                batch = [message.value for partition in events.values() for message in partition]
                
                logger.info(f"Processing batch of {len(batch)} events")
                process_batch(batch)
                
                # Manually commit after successful processing
                consumer.commit()
                logger.info("Batch successfully committed")

    except Exception as e:
        logger.error(f"Error during Kafka consumption: {e}")

    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")

if __name__ == "__main__":
    main()