# About the project

<img src="docs/images/arcane-logo.png" width="100" height="100" alt="logo"> 

**Arcane** is a Kubernetes-native data streaming platform powered by extendable plugin architecture.

Arcane is designed to be:
- **Kubernetes-native**: It runs data streaming plugins as a Kubernetes jobs and uses Kubernetes API to manage the
   lifecycle of data streams.

- **Extendable**: It allows you to extend the platform with your own data streaming plugins, which can be written in
   any programming language.

- **Scalable**: It can scale horizontally by adding more Kubernetes nodes to the cluster or vertically by adding more
   resources to the streaming jobs. It does not require any stateful components like Zookeeper or Kafka brokers which 
   makes it easy to scale and manage.

- **Lightweight**: It has a small footprint and can run on any Kubernetes cluster, including local clusters like
   [Minikube](https://minikube.sigs.k8s.io/docs/) or [Kind](https://kind.sigs.k8s.io/).
 
- **Cloud-agnostic**: It can run on any cloud provider that supports Kubernetes, including AWS, Azure, GCP, and
   on-premises clusters.

This repository contains the **Arcane Operator**, which is responsible for managing the lifecycle of Arcane data streams.

# Table of Contents
 - [Getting started](#getting-started)
   - [Verify the installation](#verify-the-installation)
 - [Streaming plugins](#streaming-plugins)
 - [Contributing](#contributing)
   - [Extending the platform with your own plugins](#extending-the-platform-with-your-own-plugins)

# Getting started
Run the following command to install the Arcane Operator in your Kubernetes cluster:

```bash
# Create a namespace for the operator installation
$ kubectl create namespace arcane

# Install the operator in the created namespace
$ helm install arcane oci://ghcr.io/sneaksanddata/helm/arcane-operator \
  --version v0.0.14 \
  --namespace arcane
```

This command creates a namespace `arcane` and installs the stream operator in it. By default, the Helm chart installs two CRDs:
- StreamClass
- StreamingJobTemplate

The resources of both kinds are being installed by the streaming plugins.

## Verify the installation

To verify the operator installation run the following command:

```bash
$ kubectl get pods -l app.kubernetes.io/name=arcane-operator --namespace arcane
```

It should produce the similar output:

```bash
NAME                               READY   STATUS    RESTARTS   AGE
arcane-operator-55988bbfcb-ql7qr   1/1     Running   0          25m
```

Once operator is installed, you can install the streaming plugins.

# Streaming plugins
-- TBD --

# Contributing
-- TBD --

## Extending the platform with plugins
-- TBD --
