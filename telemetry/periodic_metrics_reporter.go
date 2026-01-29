package telemetry

import (
	"context"
	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream_class"
	"sync"
)

var _ stream_class.StreamClassMetricsReporter = (*PeriodicMetricsReporter)(nil)

type PeriodicMetricsReporter struct {
	streamClassMetrics map[string]streamClassMetric
	lock               sync.RWMutex
	client             *statsd.Client
}

func (d *PeriodicMetricsReporter) RemoveStreamClass(kind string) {
	d.lock.Lock()
	defer d.lock.Unlock()

	delete(d.streamClassMetrics, kind)
}

func (d *PeriodicMetricsReporter) AddStreamClass(kind string, metricName string, tags map[string]string) {
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
func (d *PeriodicMetricsReporter) RunPeriodicMetricsReporter(ctx context.Context) {
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
	}
}

func NewPeriodicMetricsReporter(client *statsd.Client) *PeriodicMetricsReporter {
	return &PeriodicMetricsReporter{
		streamClassMetrics: make(map[string]streamClassMetric),
		client:             client,
	}
}
