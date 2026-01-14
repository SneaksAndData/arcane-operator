default:
    @just --list

fresh: stop up

up: start-kind-cluster build-deps install-integration-tests mock-stream-plugin create-mock-stream

start-kind-cluster:
    kind create cluster

stop:
    kind delete cluster

build-deps:
    helm dependency build ./integration_tests/helm/setup

install-integration-tests:
    helm upgrade --install --namespace default integration-tests integration_tests/helm/setup

mock-stream-plugin:
    helm install arcane-stream-mock oci://ghcr.io/sneaksanddata/helm/arcane-stream-mock \
        --namespace default \
        --version v0.0.0-14-g1993808

create-mock-stream:
    kubectl apply -f integration_tests/manifests/test_stream_definition.yaml

