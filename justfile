default:
    @just --list

fresh: stop up

up: start-kind-cluster build-deps integration-tests mock-stream-plugin

start-kind-cluster:
    kind create cluster

stop:
    kind delete cluster

build-deps:
    helm dependency build ./integration_tests/helm/setup

integration-tests:
    helm upgrade --install --namespace default integration-tests integration_tests/helm/setup


install-stream:
    kubectl apply -f integration_tests/manifests/stream_class.yaml
    kubectl apply -f integration_tests/manifests/crd-microsoft-sql-server-stream.yaml

mock-stream-plugin:
    helm install arcane-stream-mock oci://ghcr.io/sneaksanddata/helm/arcane-stream-mock \
        --namespace default \
        --version v1.0.1-5-g21bdef4
