/logprocessor
    types.go        ← LogEntry, Severity, LogSource interface
    processor.go    ← fan-in, filter, aggregator logic

/sources
    webserver.go    ← WebServer implements LogSource
    database.go     ← Database implements LogSource

main.go             ← wires everything together
