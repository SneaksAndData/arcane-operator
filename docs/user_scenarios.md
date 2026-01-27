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

# I want to suspend the stream
To suspend the stream, you need to update the stream custom resource (CR) and set the field `spec.suspended` to `true`.
This will stop the streaming job and prevent it from processing any new data.

# I want to start backfill for an existing stream
To start backfill for an existing stream, you need to create a backfill request custom resource (CR) that references the stream.
The backfill request will be picked up by the operator, which will create a Kubernetes job to run the backfill process.

If the stream **is suspended**, you need to create backfill request first and then unsuspend the stream.

# My stream has failed and I want to restart it
If your stream has failed, you can set `spec.suspended` to `true` to stop the stream.
To avoid data loss, you may create a backfill request that fills in any gaps occurred during the failure.
After that, you can set `spec.suspended` to `false` to restart the stream.
