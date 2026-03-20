package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	slogdatadog "github.com/samber/slog-datadog/v2"
	slogmulti "github.com/samber/slog-multi"
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

func newDatadogClient() *datadog.APIClient { // coverage-ignore
	configuration := datadog.NewConfiguration()
	apiClient := datadog.NewAPIClient(configuration)

	return apiClient
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

func ConfigureLogger(ctx context.Context, loggerConfig *DatadogLoggerConfiguration, globalTags map[string]string, logLevel string) (*slog.Logger, error) { // coverage-ignore
	slogLevel := parseSLogLevel(logLevel)

	if slogLevel == LevelLoggingDisabled {
		return slog.New(slog.DiscardHandler), nil
	}

	// in case DD logger cannot be configured, use text handler and return error, so we can warn the user they are not getting DD logs recorded
	if loggerConfig == nil {
		return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slogLevel})), nil
	}
	apiClient := newDatadogClient()
	options := slogdatadog.Option{
		Level:      slogLevel,
		Client:     apiClient,
		Context:    ctx,
		Timeout:    5 * time.Second,
		Hostname:   loggerConfig.Hostname,
		Service:    loggerConfig.ServiceName,
		GlobalTags: globalTags}

	fanoutHandler := slogmulti.Fanout(

		// first send to Datadog handler
		options.NewDatadogHandler(),

		// then to second handler: stdout
		slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slogLevel}),
	)

	return slog.New(fanoutHandler), nil
}
