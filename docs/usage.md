# Arcane Users Guide

Welcome to the Arcane Kubernetes-native data streaming platform! This guide will help you understand how to use Arcane to create and manage data streams in your Kubernetes cluster.

## Table of Contents

- [Core Concepts](#core-concepts)
  - [StreamClass](#streamclass)
  - [StreamingJobTemplate](#streamingjobtemplate)
  - [Stream Definitions](#stream-definitions)
  - [BackfillRequest](#backfillrequest)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installing Arcane Operator](#installing-arcane-operator)
  - [Installing Streaming Plugins](#installing-streaming-plugins)
- [Creating Your First Stream](#creating-your-first-stream)
- [Managing Streams](#managing-streams)
  - [Viewing Stream Status](#viewing-stream-status)
  - [Suspending Streams](#suspending-streams)
  - [Resuming Streams](#resuming-streams)
  - [Deleting Streams](#deleting-streams)
- [Backfilling Data](#backfilling-data)
- [Advanced Configuration](#advanced-configuration)
  - [Resource Limits](#resource-limits)
  - [Environment Variables and Secrets](#environment-variables-and-secrets)
  - [Job Templates](#job-templates)
- [Monitoring and Troubleshooting](#monitoring-and-troubleshooting)
  - [Checking Operator Health](#checking-operator-health)
  - [Viewing Stream Logs](#viewing-stream-logs)
  - [Common Issues](#common-issues)
- [Best Practices](#best-practices)

---

## Core Concepts

### StreamClass

A `StreamClass` is a cluster-scoped resource that tells the Arcane Operator which Custom Resource Definitions (CRDs)
to watch for stream definitions. When you install a streaming plugin, it typically creates a `StreamClass` that 
registers the plugin with the operator.

If you need to have multiple instances of the same plugin with different configurations, you can create
additional `StreamClass` resources for each plugin.

**Example StreamClass:**

```yaml
apiVersion: streaming.sneaksanddata.com/v1
kind: StreamClass
metadata:
  name: sqlserver-change-tracking-stream
spec:
  # The API group of the stream CRD to watch
  apiGroupRef: streaming.sneaksanddata.com
  
  # The API version
  apiVersion: v1
  
  # The kind of the stream CRD
  kindRef: SqlServerChangeTrackingStream
  
  # The plural name used in API paths
  pluralName: sqlserverchangetrackingstreams
  
  # Secret fields to extract and pass to jobs (optional)
  # These fields will be looked up in the referenced secret in stream definitions
  # Typically used for connection strings, API keys, etc.
  # This field is usually defined by the plugin.
  secretRefs:
    - connectionString
    - storageAccountKey
```

**Key Fields:**
- `apiGroupRef`, `apiVersion`, `kindRef`, `pluralName`: Define which CRD to watch
- `secretRefs`: List of secret fields that should be extracted and passed to streaming jobs. SecretRefs in the plugin
should be defined in the format that can be deserialized into the
[EnvFromSource](https://pkg.go.dev/k8s.io/api/core/v1#EnvFromSource) object.

### StreamingJobTemplate

A `StreamingJobTemplate` is a namespaced resource that defines the Kubernetes Job template used to run a stream.
It contains the container image, resource limits, environment variables, and other job specifications.

**Example StreamingJobTemplate:**

```yaml
apiVersion: streaming.sneaksanddata.com/v1
kind: StreamingJobTemplate
metadata:
  name: sqlserver-stream-template
  namespace: data-streaming
spec:
  # Standard Kubernetes Job spec
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: stream-processor
        image: ghcr.io/sneaksanddata/arcane-stream-sqlserver-change-tracking:v1.0.0
        resources:
          limits:
            memory: "2Gi"
            cpu: "1000m"
          requests:
            memory: "1Gi"
            cpu: "500m"
        env:
        - name: LOG_LEVEL
          value: "INFO"
        - name: METRICS_ENABLED
          value: "true"
```

### Stream Definitions

Stream Definitions are custom resources defined by streaming plugins. Each plugin creates its own CRD with specific
fields for that data source or sink. The operator watches these resources and creates/manages Kubernetes
Jobs based on their specifications.

**Example Stream Definition (conceptual):**

```yaml
apiVersion: streaming.sneaksanddata.com/v1
kind: SqlServerChangeTrackingStream
metadata:
  name: my-database-stream
  namespace: data-streaming
spec:
  # Reference to the job template (ObjectReference)
  jobTemplateRef:
    name: sqlserver-stream-template
    namespace: data-streaming  # optional, defaults to stream's namespace
  
  # Plugin-specific configuration
  sourceDatabase: MyProductionDB
  targetStorage: abfs://data@mystorageaccount.dfs.core.windows.net/tables
  
  # Secrets reference
  secretRef:
    name: database-credentials
  
  # Optional: suspend the stream
  suspended: false
  
  # Backfill job template reference (optional, ObjectReference)
  backfillJobTemplateRef:
    name: sqlserver-backfill-template
    namespace: data-streaming  # optional, defaults to stream's namespace
```

### BackfillRequest

A `BackfillRequest` is used to trigger a one-time backfill job for a stream.
This is useful when you need to reprocess historical data or recover from a failure.

**Example BackfillRequest:**

```yaml
apiVersion: streaming.sneaksanddata.com/v1
kind: BackfillRequest
metadata:
  name: backfill-2024-01
  namespace: data-streaming
spec:
  # Name of the StreamClass
  streamClass: sqlserver-change-tracking-stream
  
  # ID/name of the specific stream to backfill
  streamId: my-database-stream
  
  # Whether the backfill is completed
  completed: false
```

---

## Getting Started

### Prerequisites

- A Kubernetes cluster (v1.20 or later recommended)
- `kubectl` configured to access your cluster
- `helm` (v3.0 or later)
- Sufficient permissions to create cluster-scoped resources (CRDs, ClusterRoles, etc.)

### Installing Arcane Operator

1. **Create a namespace for Arcane:**

```bash
kubectl create namespace arcane
```

2. **Install the Arcane Operator using Helm:**

```bash
helm install arcane oci://ghcr.io/sneaksanddata/helm/arcane-operator \
  --version v0.0.14 \
  --namespace arcane
```

3. **Verify the installation:**

```bash
kubectl get pods -n arcane
```

You should see the operator pod running:

```
NAME                               READY   STATUS    RESTARTS   AGE
arcane-operator-55988bbfcb-ql7qr   1/1     Running   0          1m
```

4. **Check that the CRDs are installed:**

```bash
kubectl get crd | grep streaming
```

You should see:

```
streamclasses.streaming.sneaksanddata.com
streamingjobtemplate.streaming.sneaksanddata.com
backfillrequests.streaming.sneaksanddata.com
```

### Installing Streaming Plugins

After installing the operator, you need to install streaming plugins for your data sources. Each plugin is installed via Helm and includes:
- Plugin-specific CRDs
- A StreamClass registration
- Default StreamingJobTemplate(s)

**Example: Installing SQL Server Change Tracking Plugin**

```bash
helm install sqlserver-plugin oci://ghcr.io/sneaksanddata/helm/arcane-stream-sqlserver-change-tracking \
  --version v1.0.0 \
  --namespace data-streaming \
  --create-namespace
```

**Available Plugins:**

- **SQL Server Change Tracking**: `arcane-stream-sqlserver-change-tracking-change-tracking`
- **Microsoft Synapse Link**: `arcane-stream-microsoft-synapse-link`
- **Parquet Files**: `arcane-stream-parquet`
- **JSON Files**: `arcane-stream-json`
- **REST API**: `arcane-stream-rest-api` (Akka-based, legacy)

---

## Creating Your First Stream

Let's create a stream step by step:

### Step 1: Prepare Credentials

Create a Kubernetes Secret with connection credentials:

```bash
kubectl create secret generic my-database-creds \
  --namespace data-streaming \
  --from-literal=connectionString="Server=myserver;Database=mydb;..." \
  --from-literal=storageAccountKey="your-storage-key"
```

### Step 2: Create a Stream Definition

Create a YAML file for your stream (e.g., `my-stream.yaml`):

```yaml
apiVersion: streaming.sneaksanddata.com/v1
kind: SqlServerChangeTrackingStream
metadata:
  name: orders-stream
  namespace: data-streaming
spec:
  jobTemplateRef:
    name: sqlserver-stream-template
    namespace: data-streaming  # optional, defaults to stream's namespace
  sourceDatabase: OrdersDB
  sourceTable: Orders
  targetStorage: abfs://data@mystorageaccount.dfs.core.windows.net/orders
  secretRef:
    name: my-database-creds
  pollInterval: 60s
```

### Step 3: Apply the Stream

```bash
kubectl apply -f my-stream.yaml
```

> [!IMPORTANT]
> When you create the stream, the operator will automatically create a Backfill request to backfill historical data if the plugin supports it.
> And once the backfill is complete, it will start the regular streaming job.

### Step 4: Verify the Stream

```bash
# Check the stream status
kubectl get sqlserverchangetrackingstreams -n data-streaming

# Watch for jobs being created
kubectl get jobs -n data-streaming -w
```

You should see:
1. A backfill job created initially (if configured)
2. A regular streaming job that runs continuously or on schedule

---

## Managing Streams

### Viewing Stream Status

**List all streams:**

```bash
kubectl get <stream-kind> -n data-streaming
```

**Get detailed information:**

```bash
kubectl describe <stream-kind> <stream-name> -n data-streaming
```

**Check stream phase:**

Stream resources typically have a `status.phase` field with values like:
- `Pending`: Initial state, waiting for first job
- `Running`: Stream job is actively running
- `Backfilling`: Backfill job is in progress
- `Failed`: Stream encountered an error
- `Suspended`: Stream is paused

### Suspending Streams

To temporarily stop a stream without deleting it:

```bash
kubectl patch <stream-kind> <stream-name> -n data-streaming \
  --type merge \
  -p '{"spec":{"suspended":true}}'
```

Or edit the YAML:

```yaml
spec:
  suspended: true
```

The operator will delete the running job but keep the stream definition.

### Resuming Streams

To resume a suspended stream:

```bash
kubectl patch <stream-kind> <stream-name> -n data-streaming \
  --type merge \
  -p '{"spec":{"suspended":false}}'
```

### Deleting Streams

To permanently delete a stream:

```bash
kubectl delete <stream-kind> <stream-name> -n data-streaming
```

This will:
1. Delete the stream definition
2. Clean up associated jobs
3. Remove any related resources

---

## Backfilling Data

Backfilling allows you to reprocess historical data for a stream.

### Creating a Backfill Request

Create a YAML file (e.g., `backfill.yaml`):

```yaml
apiVersion: streaming.sneaksanddata.com/v1
kind: BackfillRequest
metadata:
  name: orders-backfill-jan-2026
  namespace: data-streaming
spec:
  streamClass: sqlserver-change-tracking-stream
  streamId: orders-stream
  completed: false
```

Apply it:

```bash
kubectl apply -f backfill.yaml
```

### Monitoring Backfill Progress

```bash
# Check backfill status
kubectl get backfillrequest -n data-streaming

# View backfill job
kubectl get jobs -l streamId=orders-stream -n data-streaming

# Check backfill logs
kubectl logs job/orders-stream-backfill -n data-streaming
```

### Completing a Backfill

Once the backfill job completes successfully, the operator will automatically mark the `BackfillRequest` as completed.

---

## Advanced Configuration

### Resource Limits

Control resource allocation by customizing StreamingJobTemplate:

```yaml
spec:
  template:
    spec:
      containers:
      - name: stream-processor
        resources:
          limits:
            memory: "4Gi"
            cpu: "2000m"
          requests:
            memory: "2Gi"
            cpu: "1000m"
```

**Best Practices:**
- Start with conservative limits and increase based on monitoring
- Set requests lower than limits to allow efficient bin-packing
- Consider using Vertical Pod Autoscaler for automatic tuning

### Environment Variables and Secrets

**Inject environment variables:**

```yaml
spec:
  template:
    spec:
      containers:
      - name: stream-processor
        env:
        - name: LOG_LEVEL
          value: "DEBUG"
        - name: MAX_BATCH_SIZE
          value: "1000"
```

**Use secrets:**

```yaml
        env:
        - name: DB_PASSWORD
          valueFrom:
            secretKeyRef:
              name: database-credentials
              key: password
```

**Mount secret files:**

```yaml
        volumeMounts:
        - name: credentials
          mountPath: /etc/credentials
          readOnly: true
      volumes:
      - name: credentials
        secret:
          secretName: database-credentials
```

### Job Templates

You can create multiple job templates for different scenarios:

**Production template** (high resources):
```yaml
apiVersion: streaming.sneaksanddata.com/v1
kind: StreamingJobTemplate
metadata:
  name: production-template
  namespace: data-streaming
spec:
  template:
    spec:
      containers:
      - name: stream-processor
        resources:
          limits:
            memory: "8Gi"
            cpu: "4000m"
```

**Development template** (low resources):
```yaml
apiVersion: streaming.sneaksanddata.com/v1
kind: StreamingJobTemplate
metadata:
  name: dev-template
  namespace: data-streaming
spec:
  template:
    spec:
      containers:
      - name: stream-processor
        resources:
          limits:
            memory: "512Mi"
            cpu: "250m"
```

Reference the appropriate template in your stream definition:

```yaml
spec:
  jobTemplateRef:
    name: production-template  # or dev-template
```

---

## Monitoring and Troubleshooting

### Checking Operator Health

**View operator logs:**

```bash
kubectl logs -n arcane -l app.kubernetes.io/name=arcane-operator -f
```

**Check operator health endpoint:**

The operator exposes health probes on port 8888:

```bash
kubectl port-forward -n arcane deploy/arcane-operator 8888:8888
curl http://localhost:8888/healthz
curl http://localhost:8888/readyz
```

### Viewing Stream Logs

**Get logs from the current job:**

```bash
# List jobs
kubectl get jobs -n data-streaming

# View job logs
kubectl logs job/<stream-name> -n data-streaming -f
```

**View logs from previous job runs:**

```bash
# Get pods for a job (including completed ones)
kubectl get pods -n data-streaming --show-all

# View logs from a completed pod
kubectl logs <pod-name> -n data-streaming
```

---

## Best Practices

### Namespace Organization

- Create dedicated namespaces for different environments:
  ```bash
  kubectl create namespace data-streaming-sql-server
  kubectl create namespace data-streaming-json
  kubectl create namespace data-streaming-parquet
  ```

### Resource Management

- Always set resource requests and limits
- Monitor actual usage and adjust accordingly
- Use node selectors or affinities for workload placement:
  ```yaml
  spec:
    template:
      spec:
        nodeSelector:
          workload-type: data-streaming
  ```
### Naming Conventions

Use consistent, descriptive names:
```yaml
# Good
name: orders-sqlserver-to-datalake-prod

# Avoid
name: stream1
```

### Cleanup

- Delete unused streams and job templates
- Set up retention policies for completed jobs:
  ```yaml
  spec:
    ttlSecondsAfterFinished: 86400  # 24 hours
  ```

> [!NOTE]
> Operator manages the lifecycle of jobs. It can recreate jobs if they are deleted manually.
> If a job is not needed (e.g., stream is suspended), operator will delete the job.


### High Availability

- Operator does not require HA and can run as a single spot instance.
- Each streaming job is independent; ensure your cluster has sufficient resources to handle peak loads.

