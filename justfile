default:
    @just --list

fresh: stop up

up: start-kind-cluster build-deps install-integration-tests

start-kind-cluster:
    kind create cluster

stop:
    kind delete cluster

build-deps:
    helm dependency build ./integration_tests/helm/setup

install-integration-tests:
    helm upgrade --install --namespace default integration-tests integration_tests/helm/setup
