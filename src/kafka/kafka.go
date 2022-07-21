package kafka

import (
	"encoding/binary"
	"strconv"

	"github.com/gauravds/goavro-withunion"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

var kafkaBrokers []string

//Setup ...
func Setup(brokers []string, schemaRegistryURL string) {
	kafkaBrokers = brokers
	NewRegistry(schemaRegistryURL)
}

//DecodeMessage either avro or json returns (decodeMsg, isAvro, schemaID, error)
func DecodeMessage(message kafka.Message) ([]byte, bool, uint32, error) {
	if message.Value != nil && message.Value[0] == 0x0 {
		schemaID := binary.BigEndian.Uint32(message.Value[1:5])
		logrus.Debug("Schema ID:", schemaID)

		schema, err := GetSchemaByID(strconv.FormatUint(uint64(schemaID), 10))
		if err != nil {
			logrus.Error("Avro Schema error:", err)
			return nil, true, schemaID, err
		}
		codec, err := goavro.NewCodec(schema)
		if err != nil {
			logrus.Error("Avro NewCodec error:", err)
			return nil, true, schemaID, err
		}

		native, _, err := codec.NativeFromBinary(message.Value[5:])
		if err != nil {
			logrus.Error("Avro NativeFromBinary error:", err)
			return nil, true, schemaID, err
		}

		textMsg, err := codec.TextualFromNative(nil, native)
		if err != nil {
			logrus.Error("Avro TextualFromNative error:", err)
			return nil, true, schemaID, err
		}
		return textMsg, true, schemaID, nil
	}

	return message.Value, false, 0, nil
}

func ConvertIfRequire(message kafka.Message, oldId, newId int) (bool, []byte) {
	_, isAvro, id, err := DecodeMessage(message)
	if isAvro && id == uint32(oldId) && err != nil {
		schemaIDBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(schemaIDBytes, uint32(newId))

		var recordValue []byte
		recordValue = append(recordValue, byte(0))
		recordValue = append(recordValue, schemaIDBytes...)
		recordValue = append(recordValue, message.Value[5:]...)

		return true, recordValue
	}
	return false, nil
}
