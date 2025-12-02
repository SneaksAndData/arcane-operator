package telemetry

import (
	"context"
	"errors"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	slogdatadog "github.com/samber/slog-datadog/v2"
	slogmulti "github.com/samber/slog-multi"
	"log/slog"
	"os"
	"time"
)

// LoggingDisabled holds a value to use when logging should be globally disabled, without removing the log handler
const LoggingDisabled = "DISABLED"

// LevelLoggingDisabled sets the slog log level for LoggingDisabled string constant
const LevelLoggingDisabled slog.Level = 100

type DatadogLoggerConfiguration struct {
	Endpoint    string
	ApiKey      string
	Hostname    string
	ServiceName string
}

func NewDatadogLoggerConfiguration() (*DatadogLoggerConfiguration, error) { // coverage-ignore
	if apiKey, endpoint, hostname, serviceName := os.Getenv("DATADOG__API_KEY"), os.Getenv("DATADOG__ENDPOINT"), os.Getenv("DATADOG__APPLICATION_HOST"), os.Getenv("DATADOG__SERVICE_NAME"); apiKey != "" && endpoint != "" && hostname != "" && serviceName != "" {
		return &DatadogLoggerConfiguration{
			Endpoint:    endpoint,
			ApiKey:      apiKey,
			Hostname:    hostname,
			ServiceName: serviceName,
		}, nil
	}

	return nil, errors.New("datadog API Key, Endpoint or Hostname and Service Name is not set")
}

func newDatadogClient(endpoint string, apiKey string, rootCtx context.Context) (*datadog.APIClient, context.Context) { // coverage-ignore
	ctx := datadog.NewDefaultContext(rootCtx)
	ctx = context.WithValue(
		ctx,
		datadog.ContextAPIKeys,
		map[string]datadog.APIKey{"apiKeyAuth": {Key: apiKey}},
	)
	ctx = context.WithValue(
		ctx,
		datadog.ContextServerVariables,
		map[string]string{"site": endpoint},
	)
	configuration := datadog.NewConfiguration()
	apiClient := datadog.NewAPIClient(configuration)

	return apiClient, ctx
}

func parseSLogLevel(levelText string) slog.Level { // coverage-ignore
	if levelText == LoggingDisabled {
		return LevelLoggingDisabled
	}

	var level slog.Level
	var err = level.UnmarshalText([]byte(levelText))
	if err != nil {
		return slog.LevelInfo
	}

	return level
}

func ConfigureLogger(ctx context.Context, globalTags map[string]string, logLevel string) (*slog.Logger, error) { // coverage-ignore
	slogLevel := parseSLogLevel(logLevel)

	if slogLevel == LevelLoggingDisabled {
		return slog.New(slog.DiscardHandler), nil
	}

	loggerConfig, err := NewDatadogLoggerConfiguration()
	// in case DD logger cannot be configured, use text handler and return error, so we can warn the user they are not getting DD logs recorded
	if err != nil {
		return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slogLevel})), err
	}
	apiClient, ctx := newDatadogClient(loggerConfig.Endpoint, loggerConfig.ApiKey, ctx)
	return slog.New(
		slogmulti.Fanout(
			slogdatadog.Option{Level: slogLevel, Client: apiClient, Context: ctx, Timeout: 5 * time.Second, Hostname: loggerConfig.Hostname, Service: loggerConfig.ServiceName, GlobalTags: globalTags}.NewDatadogHandler(), // first send to Datadog handler
			slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slogLevel}), // then to second handler: stdout
		),
	), nil
}
