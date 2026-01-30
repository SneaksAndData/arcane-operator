package stream_class

import (
	"context"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// StreamControllerHandle holds the cancel function and GVK for a stream controller
type StreamControllerHandle struct {
	cancelFunc context.CancelFunc
	gvk        schema.GroupVersionKind
}
