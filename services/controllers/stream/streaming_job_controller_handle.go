package stream

import (
	"context"
	"fmt"
	"github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers/common"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream_class"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"reflect"
)

var _ stream_class.StreamControllerHandle = (*ControllerHandle)(nil)

type ControllerHandle struct {
	factory      informers.SharedInformerFactory
	class        *v1.StreamClass
	context      context.Context
	cancelFunc   context.CancelFunc
	sharedQueue  workqueue.TypedRateLimitingInterface[StreamEvent]
	informer     informers.GenericInformer
	registration cache.ResourceEventHandlerRegistration
	logger       klog.Logger
}

func NewControllerHandle(logger klog.Logger, factory informers.SharedInformerFactory, class *v1.StreamClass, q QueueProvider) *ControllerHandle {
	ctx, cancelFunc := context.WithCancel(context.Background())
	return &ControllerHandle{
		factory:     factory,
		class:       class,
		context:     ctx,
		cancelFunc:  cancelFunc,
		sharedQueue: q.GetQueue(),
		logger:      logger,
	}
}

func (s ControllerHandle) Start() error {
	if s.informer != nil {
		return nil // Already started
	}
	informer, err := s.factory.ForResource(schema.GroupVersionResource{
		Group:    s.class.Spec.APIGroupRef,
		Version:  s.class.Spec.APIVersion,
		Resource: s.class.Spec.PluralName,
	})
	if err != nil {
		return fmt.Errorf("failed to create informer: %w", err)
	}

	registration, err := informer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			event, err := tryFromKubernetesObject(obj)
			if err != nil {
				s.logger.V(0).Error(err, "failed to parse event")
			}
			s.sharedQueue.Add(event)
		},
		UpdateFunc: func(oldObj, newObj any) {
			event, err := tryFromKubernetesObject(newObj)
			if err != nil {
				s.logger.V(0).Error(err, "failed to parse event")
			}
			s.sharedQueue.Add(event)
		},
	})
	if err != nil {
		return fmt.Errorf("error adding StreamClass controller: %w", err)
	}

	s.informer = informer
	s.registration = registration
	s.factory.Start(s.context.Done())
	return nil
}

func (s ControllerHandle) IsUpdateNeeded(obj *v1.StreamClass) bool {
	return reflect.DeepEqual(s.class.Spec, obj.Spec)
}

func (s ControllerHandle) Id() common.StreamClassWorkerIdentity {
	return s.class
}

func (s ControllerHandle) Stop() error {
	s.cancelFunc()
	// Here we will wait for the informer and the operator to fully stop

	return nil
}
