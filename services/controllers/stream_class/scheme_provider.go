package stream_class

import "k8s.io/apimachinery/pkg/runtime"

type SchemeProvider interface {
	GetScheme() *runtime.Scheme
}
