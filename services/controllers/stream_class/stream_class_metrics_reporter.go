package stream_class

// StreamClassMetricsReporter defines the interface for reporting metrics related to stream classes.
type StreamClassMetricsReporter interface {

	// AddStreamClass registers a new stream class with the given kind, metric name, and tags for metrics reporting.
	AddStreamClass(kind string, metricName string, tags map[string]string)

	// RemoveStreamClass unregisters the stream class of the specified kind from metrics reporting.
	RemoveStreamClass(kind string)
}
