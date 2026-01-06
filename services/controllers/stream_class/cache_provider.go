package stream_class

import "sigs.k8s.io/controller-runtime/pkg/cache"

type CacheProvider interface {
	GetCache() cache.Cache
}
