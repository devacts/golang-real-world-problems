package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type SEVERITY string

const (
	INFO  SEVERITY = "INFO"
	WARN  SEVERITY = "WARN"
	ERROR SEVERITY = "ERROR"
)

type LogEntry struct {
	Source    string
	Severity  SEVERITY
	Message   string
	Timestamp time.Time
}

type SourceStats struct {
	sevInfoCount int
	sevWarnCount int
	sevErrCount  int
}

type SafeStats struct {
	statsMu           sync.Mutex
	sourceLogCountMap map[string]SourceStats
}

func NewSafeStats() *SafeStats {
	return &SafeStats{
		sourceLogCountMap: make(map[string]SourceStats),
	}
}

func createLogEntry(source string, sev SEVERITY, message string) LogEntry {
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
			out <- createLogEntry("web-server", INFO, "Pinging Log Server")
		case <-warn:
			out <- createLogEntry("web-server", WARN, "Pong not received since 2 seconds")
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
			out <- createLogEntry("auth-service", INFO, "Pinging Log Server")
		case <-err:
			out <- createLogEntry("auth-service", ERROR, "Pong not received since 2 seconds")
		case <-ctx.Done():
			return
		}
	}
}

func (s *SafeStats) printSevCountPerSourcePerSecond(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.statsMu.Lock()
			for source, sevCounts := range s.sourceLogCountMap {
				fmt.Printf("Source %s sev counts = %v\n", source, sevCounts)
				s.sourceLogCountMap[source] = SourceStats{0, 0, 0}
			}
			s.statsMu.Unlock()
		case <-ctx.Done():
			return
		}
	}
}

func (s *SafeStats) processEmitLogs(out chan LogEntry) {
	for log := range out {
		s.statsMu.Lock()
		ss := s.sourceLogCountMap[log.Source]
		switch log.Severity {
		case INFO:
			ss.sevInfoCount++
			s.sourceLogCountMap[log.Source] = ss
		case WARN:
			ss.sevWarnCount++
			s.sourceLogCountMap[log.Source] = ss
		case ERROR:
			ss.sevErrCount++
			s.sourceLogCountMap[log.Source] = ss
		}
		s.statsMu.Unlock()
	}
}

func main() {

	out := make(chan LogEntry, 3)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	var wge, wgp, wgpr sync.WaitGroup
	wge.Go(func() {
		EmitWebServerLogs(ctx, out)
	})
	wge.Go(func() {
		EmitAuthServiceLogs(ctx, out)
	})

	s := NewSafeStats()
	wgp.Add(1)
	go func() {
		defer wgp.Done()
		s.processEmitLogs(out)
	}()

	printContext, printCancel := context.WithCancel(context.Background())
	wgpr.Go(func() {
		s.printSevCountPerSourcePerSecond(printContext)
	})

	wge.Wait()
	close(out)
	wgp.Wait()
	printCancel()
	wgpr.Wait()

	fmt.Println("Exiting...")
}
