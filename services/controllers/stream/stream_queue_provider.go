package stream

import "k8s.io/client-go/util/workqueue"

type QueueProvider interface {
	GetQueue() workqueue.TypedRateLimitingInterface[StreamEvent]
}
