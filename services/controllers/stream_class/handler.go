package stream_class

import (
	"arcane-operator/configuration/conf"
	"arcane-operator/pkg/apis/streaming/v1"
	"golang.org/x/time/rate"
	"k8s.io/client-go/util/workqueue"
	"log/slog"
)

var _ StreamClassHandler = (*StreamClassEventHandler)(nil)

type StreamClassEventHandler struct {
	logger    slog.Logger
	workQueue workqueue.TypedRateLimitingInterface[StreamClassEvent]
}

// NewStreamClassEventHandler creates a new StreamClassEventHandler
//
//lint:ignore U1000 Suppress unused constructor temporarily
func NewStreamClassEventHandler(log slog.Logger, configuration conf.StreamClassOperatorConfiguration) *StreamClassEventHandler {

	rlc := configuration.RateLimiting
	rateLimiter := workqueue.NewTypedMaxOfRateLimiter(
		workqueue.NewTypedItemExponentialFailureRateLimiter[StreamClassEvent](rlc.FailureRateBaseDelay, rlc.FailureRateMaxDelay),
		&workqueue.TypedBucketRateLimiter[StreamClassEvent]{
			Limiter: rate.NewLimiter(rlc.RateLimitElementsPerSecond, rlc.RateLimitElementsBurst),
		},
	)

	return &StreamClassEventHandler{
		logger:    log,
		workQueue: workqueue.NewTypedRateLimitingQueue(rateLimiter),
	}
}

func (s *StreamClassEventHandler) HandleStreamClassAdded(obj any) {
	if streamClass, ok := obj.(*v1.StreamClass); ok {
		s.workQueue.Add(StreamClassEvent{
			Type:        StreamClassAdded,
			StreamClass: streamClass,
		})
	} else {
		s.logger.Error("HandleStreamClassAdded: unable to cast object to StreamClass")
	}
}

func (s *StreamClassEventHandler) HandleStreamClassUpdated(_ any, newObj any) {
	if streamClass, ok := newObj.(*v1.StreamClass); ok {
		s.workQueue.Add(StreamClassEvent{
			Type:        StreamClassUpdated,
			StreamClass: streamClass,
		})
	} else {
		s.logger.Error("HandleStreamClassAdded: unable to cast object to StreamClass")
	}
}

func (s *StreamClassEventHandler) HandleStreamClassDeleted(obj any) {
	if streamClass, ok := obj.(*v1.StreamClass); ok {
		s.workQueue.Add(StreamClassEvent{
			Type:        StreamClassDeleted,
			StreamClass: streamClass,
		})
	} else {
		s.logger.Error("HandleStreamClassAdded: unable to cast object to StreamClass")
	}
}
