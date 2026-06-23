package logx

import (
	"fmt"
	"os"
	"path/filepath"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logger *zap.SugaredLogger

// Init sets up the global logger. All logs go to both stdout and the log file.
func Init() error {

	projectDir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("could not get working directory: %w", err)
	}

	// 2. Construct the path to the 'data' folder
	logDir := filepath.Join(projectDir, "data")

	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	logFile := filepath.Join(logDir, "app.log")

	// Open log file (append mode, create if not exists)
	f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	// Encoder config for human-readable output
	encoderCfg := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		MessageKey:     "msg",
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeLevel:    zapcore.CapitalColorLevelEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
	}

	// JSON encoder for the file
	fileEncoder := zapcore.NewJSONEncoder(encoderCfg)

	// Console encoder for stdout
	consoleEncoder := zapcore.NewConsoleEncoder(encoderCfg)

	// Write to both file and stdout
	fileCore := zapcore.NewCore(fileEncoder, zapcore.AddSync(f), zapcore.DebugLevel)
	consoleCore := zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), zapcore.DebugLevel)

	core := zapcore.NewTee(fileCore, consoleCore)
	baseLogger := zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1))

	logger = baseLogger.Sugar()
	return nil
}

// Logger returns the global sugared logger.
func Logger() *zap.SugaredLogger {
	if logger == nil {
		// Fallback to a no-op if Init was never called
		return zap.NewNop().Sugar()
	}
	return logger
}

func Debug(args ...interface{})                   { Logger().Debug(args...) }
func Info(args ...interface{})                    { Logger().Info(args...) }
func Warn(args ...interface{})                    { Logger().Warn(args...) }
func Error(args ...interface{})                   { Logger().Error(args...) }
func Fatal(args ...interface{})                   { Logger().Fatal(args...) }
func Debugf(template string, args ...interface{}) { Logger().Debugf(template, args...) }
func Infof(template string, args ...interface{})  { Logger().Infof(template, args...) }
func Warnf(template string, args ...interface{})  { Logger().Warnf(template, args...) }
func Errorf(template string, args ...interface{}) { Logger().Errorf(template, args...) }
func Fatalf(template string, args ...interface{}) { Logger().Fatalf(template, args...) }
