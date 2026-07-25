package pipeflow

import "log"

type Logger interface {
	Info(message string)
	Error(message string)
}

type DefaultLogger struct{}

func (l DefaultLogger) Info(message string) {
	log.Printf("[INFO] %s", message)
}

func (l DefaultLogger) Error(message string) {
	log.Printf("[ERROR] %s", message)
}
