package telemetry

import (
	"context"
	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream_class"
	"sync"
	"time"
)

var _ stream_class.StreamClassMetricsReporter = (*PeriodicMetricsReporter)(nil)

type PeriodicMetricsReporter struct {
	streamClassMetrics map[string]streamClassMetric
	lock               sync.RWMutex
	client             *statsd.Client
	settings           *PeriodicMetricsReporterConfig
}

func (d *PeriodicMetricsReporter) RemoveStreamClass(kind string) { // coverage-ignore (should be tested in integration tests)
	d.lock.Lock()
	defer d.lock.Unlock()

	delete(d.streamClassMetrics, kind)
}

func (d *PeriodicMetricsReporter) AddStreamClass(kind string, metricName string, tags map[string]string) { // coverage-ignore (should be tested in integration tests)
	d.lock.Lock()
	defer d.lock.Unlock()

	d.streamClassMetrics[kind] = streamClassMetric{
		metricName:  metricName,
		metricTags:  tags,
		metricValue: 1,
	}
}

// RunPeriodicMetricsReporter starts the metrics reporting loop for stream classes.
// It reports a metric for each registered stream class at regular intervals.
// When context is cancelled, the reporting loop exits.
func (d *PeriodicMetricsReporter) RunPeriodicMetricsReporter(ctx context.Context) { // coverage-ignore (should be tested in integration tests)
	time.Sleep(d.settings.InitialDelay)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			d.lock.RLock()
			for _, metric := range d.streamClassMetrics {
				Increment(d.client, metric.metricName, metric.metricTags)
			}
			d.lock.RUnlock()
		}
		time.Sleep(d.settings.ReportInterval)
	}
}

func NewPeriodicMetricsReporter(client *statsd.Client, settings *PeriodicMetricsReporterConfig) *PeriodicMetricsReporter { // coverage-ignore (constructor)
	return &PeriodicMetricsReporter{
		streamClassMetrics: make(map[string]streamClassMetric),
		client:             client,
		settings:           settings,
	}
}
