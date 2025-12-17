package stream

import (
	"context"
	"fmt"
	"github.com/SneaksAndData/arcane-operator/configuration/conf"
	"github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream_class"
	"golang.org/x/time/rate"
	"k8s.io/apimachinery/pkg/runtime/schema"
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

func (s *ControllerFactory) CreateStreamOperator(ctx context.Context, class *v1.StreamClass) (stream_class.StreamControllerHandle, error) {
	controllerKubeInformerFactory := informers.NewSharedInformerFactoryWithOptions(s.client, time.Second*30, informers.WithNamespace(class.Spec.TargetNamespace))
	controllerKubeInformerFactory.Start(ctx.Done())

	informer, err := controllerKubeInformerFactory.ForResource(schema.GroupVersionResource{
		Group:    class.Spec.APIGroupRef,
		Version:  class.Spec.APIVersion,
		Resource: class.Spec.PluralName,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create informer: %w", err)
	}

	return NewControllerHandle(informer, class, s.queue), nil
}
