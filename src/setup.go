package src

import (
	dot "github.com/joho/godotenv"
	"github.com/sirupsen/logrus"

	"kafka-schema-patcher/src/kafka"
)

func Setup() (err error) {
	if err = SetupDotEnv(); err != nil {
		return
	}
	config = SetupEnv()
	SetupLogLevel(config.logInJson)
	kafka.Setup(config.kafkaBrokers, config.kafkaSchemaRegistryUrl)
	return
}

//SetupDotEnv ...
func SetupDotEnv() error {
	if err := dot.Load(); err != nil {
		logrus.Error("Error loading .env file")
		return err
	}
	return nil
}

func SetupLogLevel(logInJson bool) {
	if logInJson {
		logrus.SetFormatter(&logrus.JSONFormatter{})
		logrus.SetLevel(logrus.InfoLevel)
	} else {
		logrus.SetFormatter(&logrus.TextFormatter{FullTimestamp: true, TimestampFormat: "2006-01-02 03:04:05PM"})
		logrus.SetLevel(logrus.TraceLevel)
	}
}
