package stream

import (
	"context"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var _ StatusManager = (*DefaultStatusManager)(nil)

type DefaultStatusManager struct {
	gvk              schema.GroupVersionKind
	client           client.Client
	streamClass      *v1.StreamClass
	definitionParser DefinitionParser
}

func NewDefaultStatusManager(client client.Client, gvk schema.GroupVersionKind, streamClass *v1.StreamClass, definitionParser DefinitionParser) *DefaultStatusManager {
	return &DefaultStatusManager{
		gvk:              gvk,
		client:           client,
		streamClass:      streamClass,
		definitionParser: definitionParser,
	}
}

func (s *DefaultStatusManager) UpdateStreamPhase(ctx context.Context, definition Definition, backfillRequest *v1.BackfillRequest, next Phase, eventFunc controllers.EventFunc) (reconcile.Result, error) {
	logger := s.getLogger(ctx, definition.NamespacedName())

	if definition.GetPhase() == next { // coverage-ignore
		logger.V(1).Info("Stream phase is already set", "phase", definition.GetPhase())
		return reconcile.Result{}, nil
	}

	logger.V(0).Info("updating Stream status", "from", definition.GetPhase(), "to", next)

	// Refetch the definition to ensure we have the latest version before updating status
	definition, err := GetStreamForClass(ctx, s.client, s.streamClass, definition.NamespacedName(), s.definitionParser)
	if err != nil {
		logger.V(0).Error(err, "unable to fetch Stream for status update")
		return reconcile.Result{}, err
	}
	err = definition.SetPhase(next)
	if err != nil { // coverage-ignore
		logger.V(0).Error(err, "unable to set Stream status")
		return reconcile.Result{}, err
	}

	err = definition.RecomputeConfiguration(backfillRequest)
	if err != nil { // coverage-ignore
		logger.V(0).Error(err, "unable to recompute Stream configuration hash")
		return reconcile.Result{}, err
	}

	err = definition.SetConditions(definition.ComputeConditions(backfillRequest))

	if err != nil { // coverage-ignore
		logger.V(0).Error(err, "unable to set Stream conditions")
		return reconcile.Result{}, err
	}

	statusUpdate := definition.ToUnstructured().DeepCopy()
	err = s.client.Status().Update(ctx, statusUpdate)
	if err != nil { // coverage-ignore
		logger.V(1).Error(err, "unable to update Stream status")
		return reconcile.Result{}, err
	}

	if eventFunc != nil {
		eventFunc()
	}

	return reconcile.Result{}, nil

}

func (s *DefaultStatusManager) getLogger(_ context.Context, request types.NamespacedName) klog.Logger {
	return klog.Background().
		WithName("StreamReconciler").
		WithValues("namespace", request.Namespace, "streamId", request.Name, "streamKind", s.gvk.Kind)
}
