package kafka

import (
	"fmt"

	"github.com/appootb/substratum/v2/logger"
)

type debugLogger struct{}

func (s *debugLogger) Printf(msg string, args ...interface{}) {
	logger.Debug("QUEUE.KAFKA", logger.Content{
		"DEBUG": fmt.Sprintf(msg, args...),
	})
}

type errorLogger struct{}

func (s *errorLogger) Printf(msg string, args ...interface{}) {
	logger.Error("QUEUE.KAFKA", logger.Content{
		"ERROR": fmt.Sprintf(msg, args...),
	})
}
