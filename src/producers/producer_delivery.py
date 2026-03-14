# %%
import sys
import os
sys.path.insert(0, r'C:\Users\User\Desktop\infnet\kafka\src')
import random
import json
from time import sleep
from typing import Any, Dict
from utils.generate_delivery_tracking import DeliveryTrackingGenerator

from jsonschema import validate, ValidationError
from confluent_kafka import SerializingProducer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer
from dataclasses import asdict

# Schema Registry configuration
schema_registry_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Define the JSON schema
schema_str = """
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "delivery",
  "type": "object",
  "properties": {
    "timestamp": { "type": "integer" },
    "driver_id": { "type": "string" },
    "delivery_id": { "type": "string" },
    "latitude": { "type": "number", "minimum": -90, "maximum": 90 },
    "longitude": { "type": "number", "minimum": -180, "maximum": 180 },
    "status": { "type": "string" }
  },
  "required": ["timestamp", "driver_id", "delivery_id", "latitude", "longitude", "status"]
}
"""
json_serializer = JSONSerializer(schema_str, schema_registry_client)


topic='delivery'

producer = SerializingProducer({
    'bootstrap.servers': '127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094',
    'client.id': topic,
    'acks': 'all',
    'batch.size': 500,
    'linger.ms': 100,
    'message.timeout.ms': 10_000,
    'retries': 3,
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': json_serializer
})

count_messages = 0
try:
    print(f"Producing messages to topic {topic}")
    while True:
        try:
            tracking_gen = DeliveryTrackingGenerator()
            stream = tracking_gen.generate_tracking_data()
            position = next(stream)
            delivery = asdict(position)
            producer.produce(
                topic=topic,
                key=delivery["delivery_id"],
                value=delivery
            )
            print(f"Produzindo mensagem: {delivery}")
        except KafkaException as e:
            print(f"Error sending message: {e}")
        except BufferError as e:
            producer.flush()
            print(f"Buffer cheio, aguardando para enviar mensagens... {e}")
        count_messages += 1
        sleep(random.uniform(0, 1))

except KeyboardInterrupt:
    print("done")
finally:
    # Send all pending messages
    producer.flush()

print(f"{count_messages} messages sent to topic {topic}")