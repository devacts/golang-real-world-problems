package main

import (
	"fmt"
	"time"
)

type LogEntry struct {
	Source    string
	Severity  string
	Message   string
	Timestamp time.Time
}

func createLogEntry(source string, sev string, message string) LogEntry {
	return LogEntry{
		Source:    source,
		Severity:  sev,
		Message:   message,
		Timestamp: time.Now(),
	}
}

func EmitWebServerLogs(out chan LogEntry, done chan bool) {
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()
	warn := time.After(2 * time.Second)
	for {
		select {
		case <-ticker.C:
			out <- createLogEntry("web-server", "INFO", "Pinging Log Server")
		case <-warn:
			out <- createLogEntry("web-server", "WARN", "Pong not received since 2 seconds")
		case <-done:
			return
		}
	}
}

func EmitAuthServiceLogs(out chan LogEntry, done chan bool) {
	ticker := time.NewTicker(time.Millisecond * 200)
	defer ticker.Stop()
	err := time.After(2 * time.Second)
	for {
		select {
		case <-ticker.C:
			out <- createLogEntry("auth-service", "INFO", "Pinging Log Server")
		case <-err:
			out <- createLogEntry("auth-service", "ERR", "Pong not received since 2 seconds")
			return
		case <-done:
			return
		}
	}
}

func printEmitLogs(out chan LogEntry) {
	for log := range out {
		fmt.Printf("log received from service :%s with Severity: %s\n", log.Source, log.Severity)
		fmt.Println(log.Message)
	}
}

func main() {

	out := make(chan LogEntry, 3)
	done := make(chan bool, 2)
	go EmitWebServerLogs(out, done)
	go EmitAuthServiceLogs(out, done)

	go printEmitLogs(out)

	time.Sleep(5 * time.Second)
	done <- true
	done <- true
	close(out)
	fmt.Println("Exiting...")
}
