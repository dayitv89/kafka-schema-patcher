package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

//Producer ...
type Producer struct {
	*kafka.Writer
}

//NewProducer ...
func NewProducer(topic string) *Producer {
	if len(kafkaBrokers) == 0 {
		return nil
	}
	kafkaWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers: kafkaBrokers,
		Topic:   topic,
	})
	return &Producer{kafkaWriter}
}

//Publish ...
func (p *Producer) Publish(key, value []byte) error {
	logrus.Info("Topic:", p.Writer.Stats().Topic, "\nKey:", string(key), "\nValue:", string(value))
	logrus.Info("start producing ... !!")
	msg := kafka.Message{
		Key:   key,
		Value: value,
	}
	return p.WriteMessages(context.Background(), msg)
}

//PublishArray ...
func (p *Producer) PublishArray(key []byte, value [][]byte) error {
	logrus.Info("Topic:", p.Writer.Stats().Topic, "\nKey:", string(key))
	logrus.Info("start producing ... !!")

	messages := []kafka.Message{}
	for _, v := range value {
		messages = append(messages, kafka.Message{Key: key, Value: v})
	}

	return p.WriteMessages(context.Background(), messages...)
}
