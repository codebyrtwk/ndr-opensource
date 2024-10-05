import os
import json
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch, helpers

# Initialize Elasticsearch client
es = Elasticsearch([os.environ.get("ES_HOST", "http://localhost:9200")])

def write_events_realtime():
    """
    Consume Kafka events in real-time, enrich them, and send them to Elasticsearch.
    """

    # Kafka configuration
    kafka_brokers = os.environ.get("KAFKA_BROKERS", "127.0.0.1:9092")
    tenant = "dev"

    # Initialize Kafka consumer
    consumer = KafkaConsumer(
        f'{tenant}-db-events',  # Single topic where all events are consumed
        bootstrap_servers=kafka_brokers,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id='threat-detection-group',
        auto_offset_reset='earliest',
        enable_auto_commit=False  # Disable auto-commit to control commit manually
    )

    try:
        while True:
            # Poll Kafka for new messages with a timeout of 1 second
            events = consumer.poll(timeout_ms=500)
            print("Polled events:", events)  # Log the polled events

            if not events:
                # No events, continue polling
                continue

            # Process each event in the batch
            for partition, messages in events.items():
                for message in messages:
                    event = message.value
                    event = json.loads(event)

                    # Fetch tenant information from the event
                    tenant_id = event.get("tenant_id")
                    if not tenant_id:
                        print("No tenant_id found in event, skipping...")
                        continue

                    # Enrich the event
                    enriched_event = enriched_events(event)

                    # Process and write to Elasticsearch in real-time
                    process_event_realtime(enriched_event, tenant_id)

            # Manually commit the offset after processing the batch
            consumer.commit()
            print("Offsets committed.")  # Log successful commit

    except Exception as e:
        print(f"Error processing events: {e}")

    finally:
        # Close the consumer to release resources
        consumer.close()
        print("Kafka consumer closed.")

def process_event_realtime(enriched_event, tenant_id):
    """
    Process and write a single enriched event to Elasticsearch in real-time.

    :param enriched_event: The enriched event to process.
    :param tenant_id: The tenant's ID for index naming.
    """
    # Define the tenant-specific index name
    tenant_index = f"{tenant_id}-events"  # Changed to avoid wildcard

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

def enriched_events(event):
    """
    Example enrichment logic for incoming events.

    :param event: The original event.
    :return: Enriched event.
    """
    # Example enrichment logic
    event['enriched'] = True  # Adding a simple field to indicate enrichment
    return event

if __name__ == "__main__":
    # Example usage of the write_events_realtime function
    write_events_realtime()
