package main

// run controller-gen to generate CRDs into Helm chart templates
//go:generate mockgen  -destination=./tests/mocks/stream_controller_factory.go -package=mocks github.com/SneaksAndData/arcane-operator/services/controllers/stream_class StreamControllerFactory
