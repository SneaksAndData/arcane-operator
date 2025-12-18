package stream_class

import (
	"context"
	"fmt"
	"github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers/common"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"sync"
)

type StreamControllerFactory interface {
	CreateStreamOperator(*v1.StreamClass) (StreamControllerHandle, error)
}

var _ StreamClassWorker = (*StreamDefinitionControllerManager)(nil)

type StreamDefinitionControllerManager struct {
	factory     StreamControllerFactory
	lock        *sync.RWMutex
	controllers map[common.WorkerId]StreamControllerHandle
	logger      *klog.Logger
	context     context.Context
	cancelFunc  context.CancelFunc
}

//lint:ignore U1000 Ignore unused function temporarily
func NewStreamDefinitionControllerManager(factory StreamControllerFactory, logger klog.Logger) *StreamDefinitionControllerManager {
	l := logger.WithValues("component", "StreamDefinitionControllerManager")
	ctx, cancelFunc := context.WithCancel(context.Background())
	return &StreamDefinitionControllerManager{
		factory:     factory,
		lock:        &sync.RWMutex{},
		controllers: make(map[common.WorkerId]StreamControllerHandle),
		logger:      &l,
		context:     ctx,
		cancelFunc:  cancelFunc,
	}
}

func (s StreamDefinitionControllerManager) HandleEvents(queue workqueue.TypedRateLimitingInterface[StreamClassEvent]) error {
	element, shutdown := queue.Get()

	if shutdown {
		s.cancelFunc()
		return nil
	}
	defer queue.Done(element)

	switch element.Type {
	case StreamClassAdded:
		// Handle StreamClass added event
		s.logger.V(4).Info("Handling StreamClass added event", "name", element.StreamClass.Name)
		isUpdateNeeded := s.isUpdateNeeded(element.StreamClass)
		if isUpdateNeeded {
			s.logger.Info("Handling StreamClass update or create", "name", element.StreamClass.Name)
			s.updateOrCreate(element.StreamClass)
		}

	case StreamClassUpdated:
		// Handle StreamClass updated event
		s.logger.Info("Handling StreamClass updated event", "name", element.StreamClass.Name)
		isUpdateNeeded := s.isUpdateNeeded(element.StreamClass)
		if isUpdateNeeded {
			s.logger.Info("Handling StreamClass update or create", "name", element.StreamClass.Name)
			s.updateOrCreate(element.StreamClass)
		}

	case StreamClassDeleted:
		// Handle StreamClass deleted event
		s.logger.Info("Handling StreamClass deleted event", "name", element.StreamClass.Name)
		err := s.stopWorker(element.StreamClass)
		if err != nil {
			s.logger.Error(err, "Error stopping StreamClass worker", "name", element.StreamClass.Name)
		}
	default:
		s.logger.Error(nil, "Unknown event type received")
	}

	return nil
}

func (s StreamDefinitionControllerManager) isUpdateNeeded(class *v1.StreamClass) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()

	controller, exists := s.controllers[class.WorkerId()]
	if !exists {
		return true
	}
	return controller.IsUpdateNeeded(class)
}

func (s StreamDefinitionControllerManager) updateOrCreate(class *v1.StreamClass) {
	s.lock.Lock()
	defer s.lock.Unlock()

	controller, exists := s.controllers[class.WorkerId()]
	if !exists {
		// Update or create the controller handle
		worker, err := s.factory.CreateStreamOperator(class)
		if err != nil {
			s.logger.Error(err, "Error creating StreamClass worker", "name", class.Name)
			return
		}
		s.controllers[class.WorkerId()] = worker

		return
	}

	// The second check to handle case where the controller was created after the first check
	if controller.IsUpdateNeeded(class) {
		err := controller.Stop()
		if err != nil {
			s.logger.Error(err, "Error stopping StreamClass worker", "name", class.Name)
			return
		}
		worker, err := s.factory.CreateStreamOperator(class)
		if err != nil {
			s.logger.Error(err, "Error creating StreamClass worker", "name", class.Name)
			return
		}
		s.controllers[class.WorkerId()] = worker
	}
}

func (s StreamDefinitionControllerManager) stopWorker(class *v1.StreamClass) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	controller, exists := s.controllers[class.WorkerId()]
	if !exists {
		return fmt.Errorf("stream class %s has already been stopped", class.Name)
	}

	if controller.IsUpdateNeeded(class) {
		// Update or create the controller handle
		err := controller.Stop()
		if err != nil {
			return fmt.Errorf("error stopping stream class %s: %w", class.Name, err)
		}
		s.controllers[class.WorkerId()] = nil
	}

	return nil
}
