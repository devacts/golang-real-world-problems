package main

import (
	"context"
	"fmt"
	"sync"
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

func EmitWebServerLogs(ctx context.Context, out chan LogEntry) {
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()
	warn := time.After(2 * time.Second)
	for {
		select {
		case <-ticker.C:
			out <- createLogEntry("web-server", "INFO", "Pinging Log Server")
		case <-warn:
			out <- createLogEntry("web-server", "WARN", "Pong not received since 2 seconds")
		case <-ctx.Done():
			return
		}
	}
}

func EmitAuthServiceLogs(ctx context.Context, out chan LogEntry) {
	ticker := time.NewTicker(time.Millisecond * 200)
	defer ticker.Stop()
	err := time.After(2 * time.Second)
	for {
		select {
		case <-ticker.C:
			out <- createLogEntry("auth-service", "INFO", "Pinging Log Server")
		case <-err:
			out <- createLogEntry("auth-service", "ERR", "Pong not received since 2 seconds")
		case <-ctx.Done():
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	var wge, wgp sync.WaitGroup
	wge.Go(func() {
		EmitWebServerLogs(ctx, out)
	})
	wge.Go(func() {
		EmitAuthServiceLogs(ctx, out)
	})

	wgp.Add(1)
	go func() {
		defer wgp.Done()
		printEmitLogs(out)
	}()

	wge.Wait()
	close(out)
	wgp.Wait()

	fmt.Println("Exiting...")
}
