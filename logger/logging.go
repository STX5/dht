package logger

type DebugLogger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

type NullLogger struct{}

func (l *NullLogger) Debugf(format string, args ...interface{}) {}
func (l *NullLogger) Infof(format string, args ...interface{})  {}
func (l *NullLogger) Errorf(format string, args ...interface{}) {}
