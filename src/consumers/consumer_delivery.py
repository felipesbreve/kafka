import signal
import sys
from confluent_kafka import DeserializingConsumer, KafkaException, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import StringDeserializer

schema_registry_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Mesmo schema do producer
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

json_deserializer = JSONDeserializer(schema_str)

topic = 'delivery'

consumer = DeserializingConsumer({
    'bootstrap.servers': '127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094',
    'group.id': 'delivery-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'key.deserializer': StringDeserializer('utf_8'),
    'value.deserializer': json_deserializer
})

# Graceful shutdown
running = True

def shutdown(signum, frame):
    global running
    print("\nEncerrando consumer...")
    running = False

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

consumer.subscribe([topic])
count_messages = 0

print(f"Consumindo mensagens do tópico '{topic}'...")

try:
    while running:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"Fim da partição {msg.partition()} no offset {msg.offset()}")
            else:
                raise KafkaException(msg.error())
            continue

        contact = msg.value()

        print(
            f"=====================================\n"
            f"[offset={msg.offset()} | partition={msg.partition()} | key={msg.key()}]\n"
            f"timestamp={contact['timestamp']},\n"
            f"driver_id={contact['driver_id']},\n"
            f"delivery_id={contact['delivery_id']},\n"
            f"latitude={contact['latitude']},\n"
            f"longitude={contact['longitude']},\n"
            f"status={contact['status']}\n\n"
        )

        # Commit manual após processar com sucesso
        consumer.commit(message=msg)
        count_messages += 1

except Exception as e:
    print(f"Erro inesperado: {e}")
    sys.exit(1)
finally:
    consumer.close()

print(f"\n{count_messages} mensagens consumidas do tópico '{topic}'")