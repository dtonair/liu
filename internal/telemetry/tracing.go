package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Tracer returns the engine's tracer from the global OpenTelemetry provider.
// Until an SDK + exporter is installed (e.g. OTLP) the global provider is a
// no-op, so spans are free; wiring an exporter in main turns them on without
// changing any call sites.
func Tracer() trace.Tracer { return otel.Tracer("github.com/dtonair/liu") }

// StartSpan starts a span named op with the given instance attribute and
// returns the derived context and span. Always call span.End().
func StartSpan(ctx context.Context, op, instanceID string) (context.Context, trace.Span) {
	return Tracer().Start(ctx, op, trace.WithAttributes(attribute.String("instance.id", instanceID)))
}
