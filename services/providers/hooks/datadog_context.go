package hooks

import (
	"context"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/SneaksAndData/arcane-operator/telemetry"
)

func WithDatadogContext(ctx context.Context, apiKey string, endpoint string) context.Context { // coverage-ignore (should be tested with integration tests)
	ctx = datadog.NewDefaultContext(ctx)
	ctx = context.WithValue(ctx, datadog.ContextAPIKeys, map[string]datadog.APIKey{"apiKeyAuth": {Key: apiKey}})
	ctx = context.WithValue(ctx, datadog.ContextServerVariables, map[string]string{"site": endpoint})
	return ctx
}

// WithStatsd enriches the context with a statsd client if it can be instantiated
func WithStatsd(ctx context.Context, metricsNamespace string) context.Context { // coverage-ignore
	statsdClient, err := statsd.New("", statsd.WithNamespace(metricsNamespace))
	if err == nil {
		return context.WithValue(ctx, telemetry.MetricsClientContextKey, statsdClient)
	}

	return ctx
}
