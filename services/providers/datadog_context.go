package providers

import (
	"context"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
)

func WithDatadogContext(ctx context.Context, apiKey string, endpoint string) context.Context {
	ctx = datadog.NewDefaultContext(ctx)
	ctx = context.WithValue(ctx, datadog.ContextAPIKeys, map[string]datadog.APIKey{"apiKeyAuth": {Key: apiKey}})
	ctx = context.WithValue(ctx, datadog.ContextServerVariables, map[string]string{"site": endpoint})
	return ctx
}
