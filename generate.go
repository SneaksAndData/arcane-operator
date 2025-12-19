package main

// run controller-gen to generate CRDs into Helm chart templates
//go:generate mockgen  -destination=./tests/mocks/stream_class_mocks.go -package=mocks github.com/SneaksAndData/arcane-operator/services/controllers/stream_class StreamControllerFactory,StreamControllerHandle
//go:generate mockgen  -destination=./tests/mocks/stream_mocks.go -package=mocks github.com/SneaksAndData/arcane-operator/services/controllers/stream JobManager,StreamDefinition,StreamEvent,StreamManager,StreamRepository,BackfillRequestRepository
