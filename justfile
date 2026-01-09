default:
    @just --list

fresh: stop up

up: start-kind-cluster build-deps install-integration-tests install-rbac install-job create-secret create-mock-stream

start-kind-cluster:
    kind create cluster

stop:
    kind delete cluster

build-deps:
    helm dependency build ./integration_tests/helm/setup

install-integration-tests:
    helm upgrade --install --namespace default integration-tests integration_tests/helm/setup


install-stream:
    helm upgrade --install arcane-stream-microsoft-sql-server oci://ghcr.io/sneaksanddata/helm/arcane-stream-microsoft-sql-server \
        --namespace arcane \
        --version v1.0.8

install-rbac:
    kubectl apply -f integration_tests/manifests/rbac.yaml

install-job:
    kubectl apply -f integration_tests/manifests/job_template.yaml

create-secret:
    kubectl apply -f integration_tests/manifests/mock_connection_string.yaml

create-mock-stream:
    kubectl apply -f integration_tests/manifests/mock_stream.yaml