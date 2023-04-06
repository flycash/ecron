package logger

import (
	logs "github.com/ecodeclub/log-api"
	"log"
)

var logger logs.Logger = logs.NewBuiltInLogger(log.Default())

// Default 返回整个项目默认的 logger
func Default() logs.Logger {
	return logger
}

func SetDefaultLogger(l logs.Logger) {
	logger = l
}
