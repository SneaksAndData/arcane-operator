package stream_class

import "context"

type StreamControllerHandle struct {
	cancelFunc context.CancelFunc
}
