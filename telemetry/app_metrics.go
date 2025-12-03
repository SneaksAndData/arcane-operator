package telemetry

import (
	"context"
	"fmt"
	"github.com/DataDog/datadog-go/v5/statsd"
	"time"
)

type contextKey string

const (
	// MetricsClientContextKey sets the context key for the metrics client
	MetricsClientContextKey contextKey = "metrics"
)

// convertTags converts tag map to the format accepted by Datadog Statsd client
func convertTags(tags map[string]string) []string {
	result := make([]string, 0, len(tags))
	for tagKey, tagValue := range tags {
		result = append(result, fmt.Sprintf("%s:%s", tagKey, tagValue))
	}

	return result
}

// WithStatsd enriches the context with a statsd client if it can be instantiated
func WithStatsd(ctx context.Context, metricsNamespace string) context.Context { // coverage-ignore
	statsdClient, err := statsd.New("", statsd.WithNamespace(metricsNamespace))
	if err == nil {
		return context.WithValue(ctx, MetricsClientContextKey, statsdClient)
	}

	return ctx
}

// Gauge reports a GAUGE metric using best-effort approach
func Gauge(metrics *statsd.Client, name string, value float64, tags map[string]string, rate float64) { // coverage-ignore
	if metrics != nil {
		_ = metrics.Gauge(name, value, convertTags(tags), rate)
	}
}

// GaugeDuration reports a GAUGE metric corresponding to a duration of an operation that started at a specified time, in milliseconds
func GaugeDuration(metrics *statsd.Client, name string, startedAt time.Time, tags map[string]string, rate float64) { // coverage-ignore
	if metrics != nil {
		duration := time.Since(startedAt).Milliseconds()
		_ = metrics.Gauge(name, float64(duration), convertTags(tags), rate)
	}
}

// Increment reports COUNT metric with a value of 1
func Increment(metrics *statsd.Client, name string, tags map[string]string) {
	if metrics != nil {
		_ = metrics.Incr(name, convertTags(tags), 1)
	}
}

// GetClient returns an initialized statsd client, if it exists
func GetClient(ctx context.Context) *statsd.Client {
	if client, ok := ctx.Value(MetricsClientContextKey).(*statsd.Client); ok {
		return client
	}

	return nil
}
