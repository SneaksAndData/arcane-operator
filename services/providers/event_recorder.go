package providers

import (
	corev1 "k8s.io/api/core/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

func NewEventRecorder(mgr manager.Manager) (record.EventRecorder, error) { // coverage-ignore (should be tested in integration tests)
	clientSet, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		return nil, err
	}

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: clientSet.CoreV1().Events("")})
	eventRecorder := eventBroadcaster.NewRecorder(apiruntime.NewScheme(), corev1.EventSource{Component: "arcane-operator"})
	return eventRecorder, nil
}
