default:
    @just --list

fresh: stop up

create-cluster:
    kind create cluster

stop:
    kind delete cluster