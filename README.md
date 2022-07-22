# kafka-schema-patcher

A go lang based tool to fix the kafka avro message with old schema id to new schema id. This is useful if you migrated the kafka and schema servers and something unwanted happen as message has old schema server id and new message has new schema server id.

So this tool is helpful to transform message which has wrong schema id(old shema server id) to correct schema id and publish it to the same topic again.

See the env

```env
LOG_IN_JSON=false
KAFKA_GROUP_NAME=kafka-schema-patcher-tool
KAFKA_MESSAGE_CONCURRENCY=100
KAFKA_FETCH_MESSAGE_TIMEOUT_IN_SECOND=10
KAFKA_BROKERS=127.0.0.1:9092
KAFKA_TOPIC=kafka.topic.has_wrong_schema_id_in_few_messages
KAFKA_SCHEMA_REGISTRY_URL=http://127.0.0.1:8081
KAFKA_SCHEMA_OLD_ID=60
KAFKA_SCHEMA_NEW_ID=4
```

## Links:

**Github:** https://github.com/dayitv89/kafka-schema-patcher

**Docker:** https://hub.docker.com/r/gauravds/kafka-schema-patcher

## Build docker image locally:

`docker buildx build --platform=linux/amd64 . --no-cache -t gauravds/kafka-schema-patcher:1.1`

## Run docker image

`docker run -it --env-file .env gauravds/kafka-schema-patcher:1.1`

## Publish docker image:

`docker push gauravds/kafka-schema-patcher:1.1`
