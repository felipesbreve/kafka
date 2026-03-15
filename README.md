# kafka

Para conectar o Redis tem que rodar o comando
```
curl -X POST -H "Content-Type: application/json" --data '{
  "name": "delivery",
  "config": {
    "connector.class": "com.redis.kafka.connect.RedisSinkConnector",
    "tasks.max": "2",
    "topics": "delivery",
    "redis.uri": "redis://172.20.0.6:6379",
    "redis.type": "JSON",
    "redis.keyspace": "delivery:delivery_id",
    "value.converter": "io.confluent.connect.json.JsonSchemaConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter.schemas.enable": "false"
  }
}' http://172.20.0.10:8083/connectors
```
