// Package telemetry provides logging, metrics, and tracing wiring for the engine.
package telemetry

import (
	"log/slog"
	"os"
)

// NewLogger returns a structured JSON logger at the given level ("debug",
// "info", "warn", "error"). Unknown levels default to info.
func NewLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	return slog.New(h)
}
