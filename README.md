# Arcane Operator

Arcane is a Kubernetes-native, lightweight, and easy-to-use data streaming platform powered by Akka.Net.

This repository contains the Arcane Operator, which is a Kubernetes operator that manages the lifecycle of
Arcane data streams.

The Arcane operator internally consists of the set of `*Operator` services
(`StreamClassOperatorService`, `StreamingJobOperatorService` and so forth)
that are listening to the Kubernetes API events and produces a commands that are handled by the
`*CommandHandler` services (`AnnotationCommandHandler`, `StreamingJobCommandHandler` and so forth).

## Project structure

The project is structured as follows:

- `.github/` contains GitHub Actions workflows

- `.container/` contains the Dockerfile for the operator image

- `src/` contains the source code of the operator
  - `Configurations/` contains the configuration models used by the operator
  - `Extensions/` contains extension methods for the operator
  - `Models/` contains the models used by the operator
    - `Api/` contains models and services for interacting with the Kubernetes API
    - `Base/` contains base classes for the operator
    - `Commands/` command models produced by the `*Operator` services
    - `Resources/` contains the models for the Kubernetes resources managed by the operator:
      1. Streaming job templates
      2. Resource statuses
      3. Stream Classes
      4. Stream Definitions
      
      Each directory inside the `Resources/` directory contains the `Base` subdirectory with interface that implemented
      by corresponding model class and the version with the actual model class version(s).
    
  - `Services/` contains the services used by the operator
    - `Base/` contains the interfaces for the classes defined in the `Services/` directory
    - `CommandHandlers/` contains the command handlers for the operator
    - `HostedServices/` contains the background services run by the operator
    - `Metrics/` contains services related to metrics publishing
    - `Operators/` contains the operator services:
      - `StreamClassOperatorService` manages the lifecycle of Stream Classes
      - `StreamingJobOperatorService` manages streaming Jobs
      - `StreamOperatorService` manages the stream definitions lifecycle
    - `Repositories/` contains the abstractions for reading and writing resources to the Kubernetes API
  - `Contracts/` contains definitions of the Kubernetes labels and annotations used by the operator
- `tests/` contains unit tests for the operator

