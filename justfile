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
    kubectl apply -f integration_tests/manifests/stream_class.yaml
    kubectl apply -f integration_tests/manifests/crd-microsoft-sql-server-stream.yaml

install-rbac:
    kubectl apply -f integration_tests/manifests/rbac.yaml

install-job:
    kubectl apply -f integration_tests/manifests/job_template.yaml

create-secret:
    kubectl apply -f integration_tests/manifests/mock_connection_string.yaml

create-mock-stream:
    kubectl apply -f integration_tests/manifests/mock_stream.yaml