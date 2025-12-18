package stream_class

import (
	"context"
	"github.com/SneaksAndData/arcane-operator/configuration/conf"
	"github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"golang.org/x/time/rate"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

type StreamClassWorker interface {
	HandleEvent(queue workqueue.TypedRateLimitingInterface[StreamClassEvent])
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
	logger klog.Logger,
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
		logger:    logger,
		workQueue: queue,
		worker:    worker,
	}
}

func (s *StreamClassEventHandler) Start(ctx context.Context) {
	go wait.UntilWithContext(ctx, func(_ context.Context) {
		s.worker.HandleEvent(s.workQueue)
	}, 0)
}

func (s *StreamClassEventHandler) HandleStreamClassAdded(obj any) {
	if streamClass, ok := obj.(*v1.StreamClass); ok {
		s.workQueue.Add(StreamClassEvent{
			Type:        StreamClassAdded,
			StreamClass: streamClass,
		})
	} else { // coverage-ignore
		s.logger.Error(nil, "HandleStreamClassAdded: unable to cast object to StreamClass")
	}
}

func (s *StreamClassEventHandler) HandleStreamClassUpdated(_ any, newObj any) {
	if streamClass, ok := newObj.(*v1.StreamClass); ok {
		s.workQueue.Add(StreamClassEvent{
			Type:        StreamClassUpdated,
			StreamClass: streamClass,
		})
	} else { // coverage-ignore
		s.logger.Error(nil, "HandleStreamClassAdded: unable to cast object to StreamClass")
	}
}

func (s *StreamClassEventHandler) HandleStreamClassDeleted(obj any) {
	if streamClass, ok := obj.(*v1.StreamClass); ok {
		s.workQueue.Add(StreamClassEvent{
			Type:        StreamClassDeleted,
			StreamClass: streamClass,
		})
	} else { // coverage-ignore
		s.logger.Error(nil, "HandleStreamClassAdded: unable to cast object to StreamClass")
	}
}
