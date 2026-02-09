# Performance Testing Resources for Arcane Operator

## Overview

This directory contains Kubernetes manifests for performance testing the Arcane operator. The resources enable automated testing of streaming workloads at scale.

## Prerequisites

- Kubernetes cluster with Arcane operator and `arcane-stream-mock` installed
- `kubectl` configured to access the target cluster
- Namespace `arcane-stream-mock` created

## Resources

### 1. test_secret.yaml
A Kubernetes Secret containing test credentials and configuration data.

**Purpose**: Provides ability to test secret management and access by streaming jobs.

**Apply**:
```bash
kubectl apply -f test_secret.yaml
```

### 2. stream_template.yaml
A `TestStreamDefinition` resource that defines mock streaming workloads.

**Purpose**: Creates test streams for performance evaluation.

**Key Parameters**:
- `runDuration`: Duration for each test run (default: 360s)
- `suspended`: Set to `false` to start streams in backfill mode
- `shouldFail`: Set to `true` to test failure scenarios

**Apply**:
```bash
kubectl apply -f stream_template.yaml
```

## Performance Testing Workflow

### Step 1: Deploy Test Secret
```bash
kubectl apply -f test_secret.yaml
```

Verify the secret is created:
```bash
kubectl get secret test-secret -n arcane-stream-mock
```

### Step 2: Create StreamingJobTemplate
Ensure the referenced `StreamingJobTemplate` (`arcane-stream-mock-standard-job`) exists in the `arcane-stream-mock` namespace before applying the stream template.

### Step 3: Deploy Test Streams
```bash
for i in {1..2000}; do kubectl create -f stream_template.yaml; done
```

This creates a two thousand test streams with a generated name (e.g., `mock-stream-abc123`).

### Step 4: Activate Test Streams
By default, streams are created in suspended state. To start testing:

Assuming you want to activate 300 streams at a time in parallel with 4 concurrent processes:
```bash
kubectl get teststreamdefinition -n arcane-stream-mock --no-headers \
  | grep Suspended \
  | head -n 300 \
  | awk '{print $1}' \
  | xargs --max-procs=4 \
  | kubectl patch teststreamdefinition <stream-name> -n arcane-stream-mock -p '{"spec":{"suspended":false}}' --type=merge
```

### Step 5: Monitor Performance
Use the Prometeus metrics exposed by the Arcane operator to monitor performance and resource usage during testing.

## Scaling Tests

## Customizing Tests

### Modify Run Duration
Edit `stream_template.yaml` and change `runDuration`:
```yaml
spec:
  runDuration: 600s  # 10 minutes
```

### Test Failure Scenarios
```yaml
spec:
  shouldFail: true
```

## Cleanup

### Remove All Test Streams
```bash
kubectl delete teststreamdefinition -n arcane-stream-mock --all
```

### Remove Test Secret
```bash
kubectl delete -f test_secret.yaml
```

## Troubleshooting

### Stream Not Starting
- Check if `suspended` is set to `false`
- Verify `StreamingJobTemplate` reference exists
- Check operator logs: `kubectl logs -n <operator-namespace> -l app=arcane-operator`

### Jobs Failing
- Verify test secret is properly configured
- Check job logs: `kubectl logs -n arcane-stream-mock <job-pod-name>`
- Ensure sufficient cluster resources (CPU, memory)
- Check if `shouldFail` is set to `true` for intentional failures

## Metrics Collection

Monitor operator performance metrics:
```bash
# CPU/Memory usage
kubectl top pods -n <operator-namespace>

# Stream processing metrics
kubectl describe teststreamdefinition -n arcane-stream-mock
```

## Best Practices

1. **Incremental Load Testing**: Start with 1-2 streams, gradually increase
2. **Resource Limits**: Monitor cluster resources during tests
3. **Cleanup**: Always clean up test resources after performance testing
4. **Isolation**: Use dedicated namespace for performance tests
5. **Baseline**: Establish baseline metrics before scaling tests
