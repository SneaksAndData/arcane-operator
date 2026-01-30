package main

//go:generate mockgen -destination=./tests/mocks/unmanaged_controller_factory.go -package=mocks github.com/SneaksAndData/arcane-operator/services/controllers/stream_class UnmanagedControllerFactory
//go:generate mockgen -destination=./tests/mocks/unmanaged_reconciler.go -package=mocks github.com/SneaksAndData/arcane-operator/services/controllers UnmanagedReconciler
//go:generate mockgen -destination=./tests/mocks/controller.go -package=mocks sigs.k8s.io/controller-runtime/pkg/controller Controller
//go:generate mockgen -destination=./tests/mocks/job_builder.go -package=mocks github.com/SneaksAndData/arcane-operator/services/controllers/stream JobBuilder
//go:generate mockgen -destination=./tests/mocks/job_mock/secret_reference_provider.go -package=mocks github.com/SneaksAndData/arcane-operator/services/job SecretReferenceProvider
//go:generate mockgen -destination=./tests/mocks/metrics_reporter.go -package=mocks github.com/SneaksAndData/arcane-operator/services/controllers/stream_class StreamClassMetricsReporter
