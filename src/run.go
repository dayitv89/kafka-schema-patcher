package src

import (
	"context"
	"fmt"
	"schema-patcher/src/kafka"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

var mustCloseRoutine = false

var signalName string
var firstSigTime time.Time
var runningTime time.Time

//Run ...
func Run() {
	topic := config.kafkaTopic
	group := config.kafkaGroupName

	consumer := kafka.NewConsumer(context.Background(), topic, group, config.kafkaMessageConcurrency, config.kafkaFetchMessageTimeoutInSecond)
	defer func() {
		logrus.Info(fmt.Sprintf("stop consuming ... !! topic: %s", topic))
		consumer.Close()
		logrus.Info(fmt.Sprintf("service stop. !! topic: %s", topic))
	}()

	gracefully := make(chan bool)
	go fetchMessages(consumer, topic, gracefully)

	sig := consumer.WaitForOSInterrupt()
	sigTime := time.Now()
	if firstSigTime.IsZero() {
		firstSigTime = sigTime
		signalName = sig.String()
	}
	logrus.Info(fmt.Sprintf("Signal termination received:%s at:%v on topic:%s", signalName, sigTime.UTC(), topic))
	mustCloseRoutine = true
	<-gracefully
	since := time.Since(sigTime)
	logrus.Info(fmt.Sprintf("gracefully service closed: %s at:%v on topic:%s closed in:%v", signalName, sigTime.UTC(), topic, since))
}

func fetchMessages(c *kafka.Consumer, topic string, gracefully chan bool) {
	logrus.Info(" [*] Waiting for new kafka messages:", topic)
	for {
		if mustCloseRoutine {
			break
		}

		fetchStartTime := time.Now()
		messages := c.FetchMessages(config.kafkaMessageConcurrency, config.kafkaFetchMessageTimeoutInSecond)

		if len(messages) > 0 {
			handleMessageCount := 0
			fm, lm := messages[0], messages[len(messages)-1]
			totalTimeTaken, execTime := timeCalc(fetchStartTime, time.Duration(0))
			logID := fmt.Sprintf("Kafka message at topic: %s, count: %d, Offset: %v-%v to %v-%v", c.Topic, len(messages), fm.Partition, fm.Offset, lm.Partition, lm.Offset)
			logrus.Warn(fmt.Sprintf("%s, state: received in %v", logID, execTime))

			totalNewMsgs := [][]byte{}
			for _, m := range messages {
				if m.Value != nil && len(m.Value) > 0 {
					handleMessageCount++

					if msg := handleMessage(m, c.Group); msg != nil && len(msg) != 0 {
						totalNewMsgs = append(totalNewMsgs, msg)
					}
				}
			}

			if len(totalNewMsgs) > 0 {
				totalTimeTaken, execTime = timeCalc(fetchStartTime, totalTimeTaken)
				logrus.Info(fmt.Sprintf("%s, state: new msg found to republish in %v(total time: %v)", logID, execTime, totalTimeTaken))

				//TODO: to publish msgs
			}

			c.CommitMessages(c.Context, messages...)
			totalTimeTaken, execTime = timeCalc(fetchStartTime, totalTimeTaken)
			logrus.Warn(fmt.Sprintf("%s, state: commit in %v(total time: *%v*)", logID, execTime, totalTimeTaken))
			logrus.Info(" [*] Waiting for new kafka messages:", topic)
		}

	}
	logrus.Info(" [*] gracefully stop Waiting for new kafka messages:", topic)
	gracefully <- true
}

func handleMessage(m kafkago.Message, group string) []byte {
	defer func() {
		if e := recover(); e != nil {
			err := fmt.Errorf("crashed Topic: %s, for Message offset: %d, value: %s", m.Topic, m.Offset, string(m.Value))
			logrus.Panic(fmt.Sprintf("===Consumer Crashed===:\t%+v\t:==================", err))
			logrus.Panic(fmt.Sprintf("===Consumer Crashed Original Error===:\t%+v\t:==================", e))
			logrus.Panic(fmt.Sprintf("===Consumer recovered it self from error===:\t%+v\t:==================", err))
		}
	}()

	_, isAvro, id, err := kafka.DecodeMessage(m)
	if isAvro && id == uint32(config.kafkaSchemaOldId) && err != nil {
		logrus.Error("Message Decode error:", err)
		//FIXME: here we need to republish with new ID

	}

	return nil
}

func timeCalc(since time.Time, lastExecTime time.Duration) (time.Duration, time.Duration) {
	totalTimeTaken := time.Since(since)
	return totalTimeTaken, totalTimeTaken - lastExecTime
}
