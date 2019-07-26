package main

import (
	"github.com/sirupsen/logrus"
)

type logFormatter struct {
	jsonFormatter *logrus.JSONFormatter
	volumeName    string
}

func (l logFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	return l.jsonFormatter.Format(entry.WithField("volume", l.volumeName))
}

func NewLogFormatter(volumeName string) *logFormatter {
	return &logFormatter{
		jsonFormatter: &logrus.JSONFormatter{},
	}
}
