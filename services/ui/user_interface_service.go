package ui

import (
	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
)

var (
	_ stream.LifetimeService = (*UserInterfaceService)(nil)
)

// UserInterfaceService implements LifetimeService to interact with the events and conditions of stream definitions
type UserInterfaceService struct {
	recorder record.EventRecorder
}

func (u UserInterfaceService) ComputeConditions(definition stream.Definition, bfr *v1.BackfillRequest) []metav1.Condition {
	switch definition.GetPhase() {
	case stream.Pending:
		return []metav1.Condition{
			{
				Type:    "Warning",
				Status:  metav1.ConditionTrue,
				Reason:  "StreamPending",
				Message: "The stream is pending and will start soon.",
				LastTransitionTime: metav1.Time{
					Time: metav1.Now().Time,
				},
			},
		}
	case stream.Backfilling:
		return []metav1.Condition{
			{
				Type:    "Ready",
				Status:  metav1.ConditionTrue,
				Reason:  "StreamBackfilling",
				Message: "The stream is currently backfilling data, request ID: " + bfr.Name,
				LastTransitionTime: metav1.Time{
					Time: metav1.Now().Time,
				},
			},
		}
	case stream.Running:
		return []metav1.Condition{
			{
				Type:    "Ready",
				Status:  metav1.ConditionTrue,
				Reason:  "StreamRunning",
				Message: "The stream is currently running.",
				LastTransitionTime: metav1.Time{
					Time: metav1.Now().Time,
				},
			},
		}
	case stream.Suspended:
		return []metav1.Condition{
			{
				Type:    "Warning",
				Status:  metav1.ConditionTrue,
				Reason:  "StreamSuspended",
				Message: "The stream is suspended.",
				LastTransitionTime: metav1.Time{
					Time: metav1.Now().Time,
				},
			},
		}
	case stream.Failed:
		return []metav1.Condition{
			{
				Type:    "Error",
				Status:  metav1.ConditionTrue,
				Reason:  "StreamFailed",
				Message: "The stream has failed.",
				LastTransitionTime: metav1.Time{
					Time: metav1.Now().Time,
				},
			},
		}
	default:
		return []metav1.Condition{}
	}
}

func (u UserInterfaceService) RecordLifetimeEvent(definition stream.Definition, bfr *v1.BackfillRequest) {
	phase := definition.GetPhase()
	switch phase {
	case stream.Backfilling:
		u.recordBackfill(definition, bfr)
	case stream.Running:
		u.recordStreamingJobCreated(definition)
	case stream.Failed:
		u.RecordStreamingJobFailed(definition, "Job reached failed state")
	}
}

func (u UserInterfaceService) recordBackfill(definition stream.Definition, bfr *v1.BackfillRequest) {
	u.recorder.Eventf(definition.ToUnstructured(),
		"Normal",
		"BackfillRequestFound",
		"Backfill request %s found for stream %s", bfr.Name, definition.NamespacedName().String())
}

func (u UserInterfaceService) recordStreamingJobCreated(definition stream.Definition) {
	u.recorder.Eventf(definition.ToUnstructured(),
		corev1.EventTypeNormal,
		"StreamingJobCreated",
		"Streaming job %s created for stream %s", definition.NamespacedName().Name, definition.NamespacedName().String())
}

func (u UserInterfaceService) RecordStreamingJobFailed(definition stream.Definition, reason string) {
	u.recorder.Eventf(definition.ToUnstructured(),
		corev1.EventTypeWarning,
		"StreamingJobFailed",
		"Streaming job for stream %s has failed. Reason: %s", definition.NamespacedName().String(), reason)
}

func NewUserInterfaceService(recorder record.EventRecorder) *UserInterfaceService {
	return &UserInterfaceService{
		recorder: recorder,
	}
}
