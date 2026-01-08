package main

//go:generate mockgen -destination=./tests/mocks/stream_reconciler_factory.go -package=mocks github.com/SneaksAndData/arcane-operator/services/controllers/stream_class UnmanagedControllerFactory
//go:generate mockgen -destination=./tests/mocks/cache_provider.go -package=mocks github.com/SneaksAndData/arcane-operator/services/controllers/stream_class CacheProvider
//go:generate mockgen -destination=./tests/mocks/stream_reconciler.go -package=mocks github.com/SneaksAndData/arcane-operator/services/controllers UnmanagedReconciler
//go:generate mockgen -destination=./tests/mocks/controller.go -package=mocks sigs.k8s.io/controller-runtime/pkg/controller Controller
//go:generate mockgen -destination=./tests/mocks/job_builder.go -package=mocks github.com/SneaksAndData/arcane-operator/services/controllers/stream JobBuilder
