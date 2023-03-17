package logger

import "log"

type NullLogger struct{}

func (l *NullLogger) Debugf(format string, args ...interface{}) {
}
func (l *NullLogger) Infof(format string, args ...interface{}) {
}
func (l *NullLogger) Errorf(format string, args ...interface{}) {
}

type PrintLogger struct{}

func (l *PrintLogger) Debugf(format string, args ...interface{}) {
	log.Printf("[DEBUG]"+format, args...)
}
func (l *PrintLogger) Infof(format string, args ...interface{}) {
	log.Printf("[INFO]"+format, args...)
}
func (l *PrintLogger) Errorf(format string, args ...interface{}) {
	log.Printf("[ERROR]"+format, args...)
}
