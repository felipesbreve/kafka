import json
from jsonschema import validate, ValidationError
from confluent_kafka import DeserializingConsumer, KafkaException
from confluent_kafka.serialization import StringDeserializer

# Mesmo schema do producer
schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "User",
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "age": {"type": "integer", "minimum": 0, "maximum": 120},
        "email": {"type": "string"}
    },
    "required": ["name", "age", "email"]
}

# 🔹 Desserializador customizado
def json_deserializer(value, ctx):
    if value is None:
        return None

    data = json.loads(value.decode("utf-8"))

    # Validação defensiva (boa prática)
    try:
        validate(instance=data, schema=schema)
    except ValidationError as e:
        raise ValueError(f"Mensagem inválida recebida: {e.message}")

    return data


consumer_conf = {
    "bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094",
    "group.id": "users-consumer-group",
    "auto.offset.reset": "earliest",
    "key.deserializer": StringDeserializer("utf_8"),
    "value.deserializer": json_deserializer
}

consumer = DeserializingConsumer(consumer_conf)

topic = "users"
consumer.subscribe([topic])

print(f"Consumindo mensagens do tópico {topic}...")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            raise KafkaException(msg.error())

        key = msg.key()
        value = msg.value()

        print("\nMensagem recebida:")
        print("Key:", key)
        print("Value:", value)

except KeyboardInterrupt:
    print("\nEncerrando consumer...")

finally:
    consumer.close()