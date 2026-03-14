import random
import json
from time import sleep
from typing import Any, Dict

from jsonschema import validate, ValidationError
from confluent_kafka import SerializingProducer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer


# Schema Registry configuration
schema_registry_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Define the JSON schema
schema_str = """
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "User",
  "type": "object",
  "properties": {
    "name": { "type": "string" },
    "age": { "type": "integer", "minimum": 0, "maximum": 120 },
    "email": { "type": "string" }
  },
  "required": ["name", "age", "email"]
}
"""
json_serializer = JSONSerializer(schema_str, schema_registry_client)

def generate_contact() -> dict[str, any]:
    first_name = random.choice([
        "Alice", "Bob", "Charlie", "Diana", "Eve",
        "Frank", "Grace", "Hank", "Ivy", "Jack"
    ])
    last_name = random.choice([
        "Smith", "Johnson", "Williams", "Brown", "Jones",
        "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"
    ])
    message = {
        "name": f"{first_name} {last_name}",
        "age": random.randint(10, 50),
        "email": f"{first_name.lower()}.{last_name.lower()}@example.com"
    }
    return message

topic='users'

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
            contact = generate_contact()
            producer.produce(
                topic=topic,
                key=contact["email"],
                value=contact
            )
            print(f"Produzindo mensagem: {contact}")
        except KafkaException as e:
            print(f"Error sending message: {e}")
        except BufferError as e:
            producer.flush()
            print(f"Buffer cheio, aguardando para enviar mensagens... {e}")
        count_messages += 1
        print(".", end="", flush=True)
        sleep(random.uniform(0, 1))

except KeyboardInterrupt:
    print("done")
finally:
    # Send all pending messages
    producer.flush()

print(f"{count_messages} messages sent to topic {topic}")