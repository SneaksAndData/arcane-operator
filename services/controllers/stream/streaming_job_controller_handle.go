package stream

import (
	"context"
	"github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers/common"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream_class"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/util/workqueue"
	"reflect"
)

var _ stream_class.StreamControllerHandle = (*ControllerHandle)(nil)

type ControllerHandle struct {
	informer    informers.GenericInformer
	class       *v1.StreamClass
	context     context.Context
	cancelFunc  context.CancelFunc
	sharedQueue workqueue.TypedRateLimitingInterface[any]
}

func NewControllerHandle(informer informers.GenericInformer, class *v1.StreamClass, sharedQueue workqueue.TypedRateLimitingInterface[any]) *ControllerHandle {
	ctx, cancelFunc := context.WithCancel(context.Background())
	informer.Informer().Run(ctx.Done())
	return &ControllerHandle{
		informer:    informer,
		class:       class,
		context:     ctx,
		cancelFunc:  cancelFunc,
		sharedQueue: sharedQueue,
	}
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

func NewStreamingJobControllerHandle() *ControllerHandle {
	return &ControllerHandle{}
}
