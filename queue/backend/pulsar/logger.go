package pulsar

import (
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/appootb/substratum/v2/logger"
)

type logWrapper struct{}

func (s *logWrapper) SubLogger(log.Fields) log.Logger {
	return s
}

func (s *logWrapper) WithFields(fields log.Fields) log.Entry {
	return entry{}.WithFields(fields)
}

func (s *logWrapper) WithField(name string, value interface{}) log.Entry {
	return entry{}.WithField(name, value)
}

func (s *logWrapper) WithError(err error) log.Entry {
	return entry{}.WithField("ERROR", err)
}

func (s *logWrapper) Debug(args ...interface{}) {
	logger.Debug("QUEUE.PULSAR", logger.Content{
		"DEBUG": fmt.Sprint(args...),
	})
}

func (s *logWrapper) Info(args ...interface{}) {
	logger.Info("QUEUE.PULSAR", logger.Content{
		"INFO": fmt.Sprint(args...),
	})
}

func (s *logWrapper) Warn(args ...interface{}) {
	logger.Warn("QUEUE.PULSAR", logger.Content{
		"WARN": fmt.Sprint(args...),
	})
}

func (s *logWrapper) Error(args ...interface{}) {
	logger.Error("QUEUE.PULSAR", logger.Content{
		"ERROR": fmt.Sprint(args...),
	})
}

func (s *logWrapper) Debugf(format string, args ...interface{}) {
	logger.Debug("QUEUE.PULSAR", logger.Content{
		"DEBUG": fmt.Sprintf(format, args...),
	})
}

func (s *logWrapper) Infof(format string, args ...interface{}) {
	logger.Info("QUEUE.PULSAR", logger.Content{
		"INFO": fmt.Sprintf(format, args...),
	})
}

func (s *logWrapper) Warnf(format string, args ...interface{}) {
	logger.Warn("QUEUE.PULSAR", logger.Content{
		"WARN": fmt.Sprintf(format, args...),
	})
}

func (s *logWrapper) Errorf(format string, args ...interface{}) {
	logger.Error("QUEUE.PULSAR", logger.Content{
		"ERROR": fmt.Sprintf(format, args...),
	})
}

type entry map[string]interface{}

func (m entry) WithFields(fields log.Fields) log.Entry {
	for k, v := range fields {
		m[k] = v
	}
	return m
}

func (m entry) WithField(name string, value interface{}) log.Entry {
	m[name] = value
	return m
}

func (m entry) Debug(args ...interface{}) {
	logger.Debug("QUEUE.PULSAR", logger.Content{
		"DEBUG": fmt.Sprint(args...),
	})
}

func (m entry) Info(args ...interface{}) {
	logger.Info("QUEUE.PULSAR", logger.Content{
		"INFO": fmt.Sprint(args...),
	})
}

func (m entry) Warn(args ...interface{}) {
	logger.Warn("QUEUE.PULSAR", logger.Content{
		"WARN": fmt.Sprint(args...),
	})
}

func (m entry) Error(args ...interface{}) {
	logger.Error("QUEUE.PULSAR", logger.Content{
		"ERROR": fmt.Sprint(args...),
	})
}

func (m entry) Debugf(format string, args ...interface{}) {
	logger.Debug("QUEUE.PULSAR", logger.Content{
		"DEBUG": fmt.Sprintf(format, args...),
	})
}

func (m entry) Infof(format string, args ...interface{}) {
	logger.Info("QUEUE.PULSAR", logger.Content{
		"INFO": fmt.Sprintf(format, args...),
	})
}

func (m entry) Warnf(format string, args ...interface{}) {
	logger.Warn("QUEUE.PULSAR", logger.Content{
		"WARN": fmt.Sprintf(format, args...),
	})
}

func (m entry) Errorf(format string, args ...interface{}) {
	logger.Error("QUEUE.PULSAR", logger.Content{
		"ERROR": fmt.Sprintf(format, args...),
	})
}
