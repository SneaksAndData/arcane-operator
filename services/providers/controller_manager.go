package providers

import (
	"github.com/SneaksAndData/arcane-operator/config"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	controllerruntime "sigs.k8s.io/controller-runtime"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func ControllerManager(kubeconfig *rest.Config, appConfig *config.AppConfig) (controllerruntime.Manager, error) {
	return controllerruntime.NewManager(kubeconfig, controllerruntime.Options{
		Metrics: metricsserver.Options{
			BindAddress: appConfig.Telemetry.MetricsBindAddress,
		},
		Scheme: apiruntime.NewScheme(),
	})
}
