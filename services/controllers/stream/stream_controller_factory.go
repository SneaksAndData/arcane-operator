package stream

import (
	"fmt"
	"github.com/SneaksAndData/arcane-operator/configuration/conf"
	"github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream_class"
	"golang.org/x/time/rate"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"time"
)

var _ stream_class.StreamControllerFactory = (*ControllerFactory)(nil)

type ControllerFactory struct {
	logger klog.Logger
	queue  workqueue.TypedRateLimitingInterface[any]
	client kubernetes.Interface
}

//lint:ignore U1000 Ignore unused function temporarily
func NewStreamControllerFactory(logger klog.Logger, configuration conf.StreamOperatorConfiguration) *ControllerFactory {
	rlc := configuration.RateLimiting
	rateLimiter := workqueue.NewTypedMaxOfRateLimiter(
		workqueue.NewTypedItemExponentialFailureRateLimiter[any](rlc.FailureRateBaseDelay, rlc.FailureRateMaxDelay),
		&workqueue.TypedBucketRateLimiter[any]{
			Limiter: rate.NewLimiter(rlc.RateLimitElementsPerSecond, rlc.RateLimitElementsBurst),
		},
	)

	queue := workqueue.NewTypedRateLimitingQueue[any](rateLimiter)
	return &ControllerFactory{
		logger: logger,
		queue:  queue,
	}

}

func (s *ControllerFactory) CreateStreamOperator(class *v1.StreamClass) (stream_class.StreamControllerHandle, error) {
	factory := informers.NewSharedInformerFactoryWithOptions(s.client, time.Second*30, informers.WithNamespace(class.Spec.TargetNamespace))
	handle := NewControllerHandle(factory, class, s.queue)
	err := handle.Start()
	if err != nil {
		return nil, fmt.Errorf("failed to start controller for class %s: %w", class.Name, err)
	}
	return handle, nil
}
