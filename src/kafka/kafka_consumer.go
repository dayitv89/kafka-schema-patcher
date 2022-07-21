package kafka

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

//Consumer ...
type Consumer struct {
	*kafka.Reader
	Context context.Context
	Topic   string
	Group   string
}

//NewConsumer ...
func NewConsumer(ctx context.Context, topic, group string, queueCapacity int, timeout time.Duration) *Consumer {
	logrus.Info("Consumer setup with topic:", topic, " group:", group, " kafkaBrokers:", kafkaBrokers)

	dialer := &kafka.Dialer{
		Timeout:   timeout,
		DualStack: true,
	}

	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers:         kafkaBrokers,
		Topic:           topic,
		StartOffset:     kafka.FirstOffset,
		MinBytes:        1,
		MaxBytes:        100e6,
		GroupID:         group,
		ReadLagInterval: 1 * time.Second,
		Dialer:          dialer,
		QueueCapacity:   queueCapacity * 2,
	})
	return &Consumer{consumer, ctx, topic, group}
}

//FetchMessages enables service as manual commit, use ReadMessage for auto commit
func (c *Consumer) FetchMessages(count int, timeout time.Duration) []kafka.Message {
	messages := []kafka.Message{}

	t := time.Now()
	for i := 0; i < count; i++ {
		since := time.Since(t)
		if since >= timeout {
			break
		}

		ctx, _ := context.WithTimeout(c.Context, timeout-since)
		m, err := c.FetchMessage(ctx)
		if err != nil {
			switch err.Error() {
			case "context deadline exceeded":
				logrus.Info(fmt.Sprintf("No new kafka message(%s) in last %v(currentTime: %v), err:%v", c.Topic, timeout, time.Now().UTC(), err))
			case "EOF":
				break
			default:
				logrus.Error("Error while fetching kafka message:", err)
			}
		} else if m.Value != nil && len(m.Value) > 0 {
			messages = append(messages, m)
		}
	}

	return messages
}

//CommitMessage ...
func (c *Consumer) CommitMessage(msgs []kafka.Message) error {
	for _, m := range msgs {
		logrus.Debug("Message is about to commit partition:", m.Partition, " offset:", m.Offset)
	}
	return c.CommitMessages(c.Context, msgs...)
}

//WaitForOSInterrupt ...
func (c *Consumer) WaitForOSInterrupt() os.Signal {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	sig := <-signals
	return sig
}
