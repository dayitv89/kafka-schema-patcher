package src

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

var config Config

type Config struct {
	logInJson                        bool
	kafkaGroupName                   string
	kafkaMessageConcurrency          int
	kafkaFetchMessageTimeoutInSecond time.Duration
	kafkaBrokers                     []string
	kafkaTopic                       string
	kafkaSchemaRegistryUrl           string
	kafkaSchemaOldId                 int
	kafkaSchemaNewId                 int
}

func SetupEnv() Config {
	config = Config{
		logInJson:                        false,
		kafkaGroupName:                   "kafka-schema-patcher-tool",
		kafkaMessageConcurrency:          100,
		kafkaFetchMessageTimeoutInSecond: 10 * time.Second,
	}

	if logInJson := os.Getenv("LOG_IN_JSON"); logInJson == "true" {
		config.logInJson = true
	}

	if kafkaGroupName := os.Getenv("KAFKA_GROUP_NAME"); kafkaGroupName != "" {
		config.kafkaGroupName = kafkaGroupName
	}

	if kafkaMessageConcurrency := os.Getenv("KAFKA_MESSAGE_CONCURRENCY"); kafkaMessageConcurrency != "" {
		if i, err := strconv.Atoi(kafkaMessageConcurrency); err == nil {
			config.kafkaMessageConcurrency = i
		} else {
			panic(fmt.Sprintf("KAFKA_MESSAGE_CONCURRENCY not valid: %v", err))
		}
	}

	if kafkaFetchMessageTimeoutInSecond := os.Getenv("KAFKA_FETCH_MESSAGE_TIMEOUT_IN_SECOND"); kafkaFetchMessageTimeoutInSecond != "" {
		if i, err := strconv.Atoi(kafkaFetchMessageTimeoutInSecond); err == nil {
			config.kafkaFetchMessageTimeoutInSecond = time.Duration(i) * time.Second
		} else {
			panic(fmt.Sprintf("KAFKA_FETCH_MESSAGE_TIMEOUT_IN_SECOND not valid: %v", err))
		}
	}

	if kafkaBrokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ","); len(kafkaBrokers) > 0 {
		config.kafkaBrokers = kafkaBrokers
	} else {
		panic("KAFKA_BROKERS not valid")
	}

	if kafkaTopic := os.Getenv("KAFKA_TOPIC"); kafkaTopic != "" {
		config.kafkaTopic = kafkaTopic
	} else {
		panic("KAFKA_TOPIC not valid")
	}

	if kafkaSchemaRegistryUrl := os.Getenv("KAFKA_SCHEMA_REGISTRY_URL"); kafkaSchemaRegistryUrl != "" {
		config.kafkaSchemaRegistryUrl = kafkaSchemaRegistryUrl
	} else {
		panic("KAFKA_SCHEMA_REGISTRY_URL not valid")
	}

	if kafkaSchemaOldId := os.Getenv("KAFKA_SCHEMA_OLD_ID"); kafkaSchemaOldId != "" {
		if i, err := strconv.Atoi(kafkaSchemaOldId); err == nil {
			config.kafkaSchemaOldId = i
		} else {
			panic(fmt.Sprintf("KAFKA_SCHEMA_OLD_ID not valid: %v", err))
		}
	} else {
		panic("KAFKA_SCHEMA_OLD_ID not valid")
	}

	if kafkaSchemaNewId := os.Getenv("KAFKA_SCHEMA_NEW_ID"); kafkaSchemaNewId != "" {
		if i, err := strconv.Atoi(kafkaSchemaNewId); err == nil {
			config.kafkaSchemaNewId = i
		} else {
			panic(fmt.Sprintf("KAFKA_SCHEMA_NEW_ID not valid: %v", err))
		}
	} else {
		panic("KAFKA_SCHEMA_NEW_ID not valid")
	}
	return config
}
