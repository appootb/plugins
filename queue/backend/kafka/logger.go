package kafka

import (
	"fmt"

	"github.com/appootb/substratum/v2/logger"
)

type debugLogger struct{}

func (s debugLogger) Printf(msg string, args ...interface{}) {
	logger.Debug("kafka_debug", logger.Content{
		"msg":  msg,
		"args": fmt.Sprint(args...),
	})
}

type errorLogger struct{}

func (s errorLogger) Printf(msg string, args ...interface{}) {
	logger.Error("kafka_error", logger.Content{
		"msg":  msg,
		"args": fmt.Sprint(args...),
	})
}
