package stream_class

import (
	"github.com/SneaksAndData/arcane-operator/configuration/conf"
	"github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"golang.org/x/time/rate"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

type StreamClassWorker interface {
	HandleEvents(queue workqueue.TypedRateLimitingInterface[StreamClassEvent]) error
}

var _ StreamClassHandler = (*StreamClassEventHandler)(nil)

type StreamClassEventHandler struct {
	logger    klog.Logger
	workQueue workqueue.TypedRateLimitingInterface[StreamClassEvent]
	worker    StreamClassWorker
}

// NewStreamClassEventHandler creates a new StreamClassEventHandler
//
//lint:ignore U1000 Suppress unused constructor temporarily
func NewStreamClassEventHandler(
	log klog.Logger,
	configuration conf.StreamClassOperatorConfiguration,
	worker StreamClassWorker) *StreamClassEventHandler {

	rlc := configuration.RateLimiting
	rateLimiter := workqueue.NewTypedMaxOfRateLimiter(
		workqueue.NewTypedItemExponentialFailureRateLimiter[StreamClassEvent](rlc.FailureRateBaseDelay, rlc.FailureRateMaxDelay),
		&workqueue.TypedBucketRateLimiter[StreamClassEvent]{
			Limiter: rate.NewLimiter(rlc.RateLimitElementsPerSecond, rlc.RateLimitElementsBurst),
		},
	)

	queue := workqueue.NewTypedRateLimitingQueue(rateLimiter)
	return &StreamClassEventHandler{
		logger:    log,
		workQueue: queue,
		worker:    worker,
	}
}

func (s *StreamClassEventHandler) HandleStreamClassAdded(obj any) {
	if streamClass, ok := obj.(*v1.StreamClass); ok {
		s.workQueue.Add(StreamClassEvent{
			Type:        StreamClassAdded,
			StreamClass: streamClass,
		})
	} else {
		s.logger.Error(nil, "HandleStreamClassAdded: unable to cast object to StreamClass")
	}
}

func (s *StreamClassEventHandler) HandleStreamClassUpdated(_ any, newObj any) {
	if streamClass, ok := newObj.(*v1.StreamClass); ok {
		s.workQueue.Add(StreamClassEvent{
			Type:        StreamClassUpdated,
			StreamClass: streamClass,
		})
	} else {
		s.logger.Error(nil, "HandleStreamClassAdded: unable to cast object to StreamClass")
	}
}

func (s *StreamClassEventHandler) HandleStreamClassDeleted(obj any) {
	if streamClass, ok := obj.(*v1.StreamClass); ok {
		s.workQueue.Add(StreamClassEvent{
			Type:        StreamClassDeleted,
			StreamClass: streamClass,
		})
	} else {
		s.logger.Error(nil, "HandleStreamClassAdded: unable to cast object to StreamClass")
	}
}
