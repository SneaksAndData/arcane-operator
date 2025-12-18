package stream_class

import (
	"context"
	"fmt"
	v1 "github.com/SneaksAndData/arcane-operator/pkg/generated/informers/externalversions/streaming/v1"
	"k8s.io/client-go/tools/cache"
)

type StreamClassHandler interface {
	HandleStreamClassAdded(obj any)
	HandleStreamClassUpdated(oldObj any, newObj any)
	HandleStreamClassDeleted(obj any)
	Start(ctx context.Context)
}

// StreamClassController reconciles a StreamClass object
type StreamClassController struct {
	informer v1.StreamClassInformer
	handler  StreamClassHandler
}

// NewStreamClassController creates a new StreamClassController
//
//lint:ignore U1000 Suppress unused constructor temporarily
func NewStreamClassController(informer v1.StreamClassInformer, handler StreamClassHandler) (*StreamClassController, error) {
	controller := &StreamClassController{
		informer: informer,
		handler:  handler,
	}

	inf := informer.Informer()
	_, err := inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    handler.HandleStreamClassAdded,
		UpdateFunc: handler.HandleStreamClassUpdated,
		DeleteFunc: handler.HandleStreamClassDeleted,
	})

	if err != nil { // coverage-ignore
		return nil, fmt.Errorf("error adding StreamClass controller: %w", err)
	}

	return controller, nil
}

func (s *StreamClassController) Start(ctx context.Context) {
	s.handler.Start(ctx)
}
