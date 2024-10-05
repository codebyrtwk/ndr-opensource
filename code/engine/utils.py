import logger

def on_send_success(record_metadata):
    logger.info(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")

def on_send_error(excp):
    logger.error(f"Failed to send message: {excp}")