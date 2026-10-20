# Arcane user scenarios cheat sheet

## I want to create a streaming job that runs on the cluster
To create a streaming job, you need to define a stream custom resource (CR) that specifies the source and sink of the
data stream. You can use one of the available streaming plugins or create your own.

When a new stream resource is created, Arcane will create a backfill request for the stream. The backfill request
will be picked up by the operator, which will create a Kubernetes job to run the streaming job.

When the backfill process is completed, operator will create a job that will continue the stream.

## I want to create a stream without backfill
To create a stream without backfill, you need to define a stream custom resource (CR) with the field `spec.suspended`
set to `true`. After that you can unsuspend the stream to start it in the streaming mode.

## I want to suspend the stream
To suspend the stream, you need to update the stream custom resource (CR) and set the field `spec.suspended` to `true`.
This will stop the streaming job and prevent it from processing any new data.

## I want to start backfill for an existing stream
To start backfill for an existing stream, you need to create a backfill request custom resource (CR) that references 
the stream. If the stream was in a `Running` phase, the backfill request will be picked up by the operator, which will 
create a Kubernetes job to run the backfill process. If the stream was in a `Suspended` phase, you should unsuspend the 
stream first to start the backfill process. If the stream was in a `Failed` phase, you should fix the issue that caused
the failure first, and then unsuspend the stream.

Example backfill request:
```yaml
apiVersion: streaming.sneaksanddata.com/v1
kind: BackfillRequest
metadata:
  name: my-backfill-request
  namespace: arcane-stream-mock
spec:
  streamClass: arcane-stream-mock
  streamId: my-stream
```

## My stream has failed and I want to restart it
If your stream has failed, you can set `spec.suspended` to `true` to stop the stream.
To avoid data loss, you may create a backfill request that fills in any gaps occurred during the failure.
After that, you can set `spec.suspended` to `false` to restart the stream.

## I deleted the pod and my stream transitioned to failed state, how do I avoid that in the future?
Arcane streaming is built on top of Kubernetes Jobs. By default, when a pod is deleted manually or due to node eviction,
all containers in the pod receive a SIGTERM signal and have a grace period to shut down gracefully **with exit code 0**.
If the containers do not shut down within the grace period, the pod is forcefully terminated. If the pod is terminated
with a non-zero exit code, Kubernetes counts this pod **as failed** and the job may transition to a failed state.
It's a responsibility of the user and/or the plugin developer to ensure that the streaming job handles termination signals
gracefully and exits with code 0 or the exit code that returned by the plugin executable on termination is
[added to the job's podFailurePolicy](https://kubernetes.io/docs/tasks/job/pod-failure-policy/).

## I want to update a stream definition while backfill is in progress. What should I expect?
Currently, if you apply any changes to a stream definition YAML, while there is an **active** backfill request,
Operator will **restart** the backfill to apply your changes.

This behaviour will be improved once [lock-on](https://github.com/SneaksAndData/arcane-operator/issues/235) is implemented.

## What happens if I delete a backfill request while the backfill job is running?
If you delete a backfill request while the backfill job is running, the operator will stop the backfill job 
and restart it in the streaming mode.

## What happens if I suspend a stream while the backfill job is running?
If you suspend a stream while the backfill job is running, the operator will stop the backfill and **will not** mark
the backfill request as completed. The backfill request will remain in the active state and you can resume it later by
unsuspending the stream. This can be useful if the plugin supports the resumable backfill (see the documentation of the 
plugin you are using for more details).

## What should I do if the backfill job fails?
If the backfill job fails, you can check the logs of the backfill job pod to see what went wrong. You can pause the
stream by setting `spec.suspended` to `true`, fix the issue, and then resume the stream by setting `spec.suspended`
to `false`. If you need to re-run the backfill, you **should** delete the existing backfill request and create a new one.

## What if the backfill job does not have enough resources to complete?
If the backfill job does not have enough resources to complete, you should suspend the stream by setting
`spec.suspended` to `true`, and then change the backfillJobTemplate in the stream definition to request more resources.
After that, you can resume the stream by setting `spec.suspended` to `false`.
