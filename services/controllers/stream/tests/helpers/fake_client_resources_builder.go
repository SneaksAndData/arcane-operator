package helpers

import (
	"sync"

	v1 "github.com/SneaksAndData/arcane-operator/pkg/apis/streaming/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// FakeClientResourcesBuilder provides a fluent builder for accumulating
// secondary Kubernetes resources (Jobs, CronJobs, BackfillRequests, ...) that
// should be seeded into a controller-runtime fake client. The builder produces
// a single mutator function suitable for the addResources parameter of
// SetupClient / SetupClientFromBuilders.
type FakeClientResourcesBuilder struct {
	mutators  []func(*crfake.ClientBuilder)
	built     func(*crfake.ClientBuilder)
	buildOnce sync.Once
}

// NewFakeClientResourcesBuilder creates an empty FakeClientResourcesBuilder.
func NewFakeClientResourcesBuilder() *FakeClientResourcesBuilder {
	return &FakeClientResourcesBuilder{}
}

// Apply appends an arbitrary mutator function to the builder. This allows
// composing the builder with existing functional-option style helpers.
func (b *FakeClientResourcesBuilder) Apply(fn func(*crfake.ClientBuilder)) *FakeClientResourcesBuilder {
	if fn != nil {
		b.mutators = append(b.mutators, fn)
	}
	return b
}

// WithOutdatedJob seeds the fake client with a batch Job whose
// configuration-hash annotation is set to "old-hash".
func (b *FakeClientResourcesBuilder) WithOutdatedJob(n types.NamespacedName) *FakeClientResourcesBuilder {
	return b.Apply(func(client2 *crfake.ClientBuilder) {
		client2.WithObjects(&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   n.Namespace,
				Name:        n.Name,
				Annotations: map[string]string{"configuration-hash": "old-hash"},
			},
		})
	})
}

// WithOutdatedCronJob seeds the fake client with a CronJob whose
// configuration-hash annotation is set to "old-hash".
func (b *FakeClientResourcesBuilder) WithOutdatedCronJob(n types.NamespacedName) *FakeClientResourcesBuilder {
	return b.Apply(func(client2 *crfake.ClientBuilder) {
		client2.WithObjects(&batchv1.CronJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   n.Namespace,
				Name:        n.Name,
				Annotations: map[string]string{"configuration-hash": "old-hash"},
			},
		})
	})
}

// WithCompletedJob seeds the fake client with a batch Job in a completed state.
func (b *FakeClientResourcesBuilder) WithCompletedJob(n types.NamespacedName) *FakeClientResourcesBuilder {
	return b.Apply(func(client2 *crfake.ClientBuilder) {
		client2.WithObjects(&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   n.Namespace,
				Name:        n.Name,
				Annotations: map[string]string{"configuration-hash": "old-hash"},
			},
			Status: batchv1.JobStatus{
				Succeeded: 1,
				Conditions: []batchv1.JobCondition{
					{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
				},
			},
		})
	})
}

// WithFailedJob seeds the fake client with a batch Job in a failed state
// (failed twice with backoffLimit = 1).
func (b *FakeClientResourcesBuilder) WithFailedJob(n types.NamespacedName) *FakeClientResourcesBuilder {
	return b.Apply(func(client2 *crfake.ClientBuilder) {
		backoffLimit := int32(1)
		client2.WithObjects(&batchv1.Job{
			Spec: batchv1.JobSpec{BackoffLimit: &backoffLimit},
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   n.Namespace,
				Name:        n.Name,
				Annotations: map[string]string{"configuration-hash": "old-hash"},
			},
			Status: batchv1.JobStatus{
				Failed: 2,
				Conditions: []batchv1.JobCondition{
					{Type: batchv1.JobFailed, Status: corev1.ConditionTrue},
					{Type: batchv1.JobFailed, Status: corev1.ConditionTrue},
				},
			},
		})
	})
}

// WithConsistentJob seeds the fake client with a batch Job whose
// configuration-hash annotation matches the provided hash.
func (b *FakeClientResourcesBuilder) WithConsistentJob(n types.NamespacedName, hash string) *FakeClientResourcesBuilder {
	return b.Apply(func(client2 *crfake.ClientBuilder) {
		client2.WithObjects(&batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   n.Namespace,
				Name:        n.Name,
				Annotations: map[string]string{"configuration-hash": hash},
			},
		})
	})
}

// WithConsistentCronJob seeds the fake client with a CronJob whose
// configuration-hash annotation matches the provided hash.
func (b *FakeClientResourcesBuilder) WithConsistentCronJob(n types.NamespacedName, hash string) *FakeClientResourcesBuilder {
	return b.Apply(func(client2 *crfake.ClientBuilder) {
		client2.WithObjects(&batchv1.CronJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   n.Namespace,
				Name:        n.Name,
				Annotations: map[string]string{"configuration-hash": hash},
			},
		})
	})
}

// WithBackfillRequest seeds the fake client with a BackfillRequest named
// "backfill1" targeting the MockStreamDefinition identified by n.
func (b *FakeClientResourcesBuilder) WithBackfillRequest(n types.NamespacedName) *FakeClientResourcesBuilder {
	return b.Apply(func(client2 *crfake.ClientBuilder) {
		client2.WithObjects(&v1.BackfillRequest{
			ObjectMeta: metav1.ObjectMeta{Name: "backfill1", Namespace: n.Namespace},
			Spec: v1.BackfillRequestSpec{
				StreamClass: "MockStreamDefinition",
				StreamId:    n.Name,
			},
		})
	})
}

// Build returns a single mutator function that applies all accumulated
// resources to a *crfake.ClientBuilder. The result is computed on the first
// call and the same function value is returned on subsequent calls.
func (b *FakeClientResourcesBuilder) Build() func(*crfake.ClientBuilder) {
	b.buildOnce.Do(func() {
		mutators := b.mutators
		b.built = func(cb *crfake.ClientBuilder) {
			for _, fn := range mutators {
				fn(cb)
			}
		}
	})
	return b.built
}
