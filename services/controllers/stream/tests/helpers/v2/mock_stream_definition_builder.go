package v2

import (
	"sync"

	testv2 "github.com/SneaksAndData/arcane-operator/pkg/test/apis_test/streaming/v2"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// MockStreamDefinitionBuilder provides a fluent builder for constructing
// *testv2.MockStreamDefinition objects in tests.
type MockStreamDefinitionBuilder struct {
	definition *testv2.MockStreamDefinition
	built      *testv2.MockStreamDefinition
	buildOnce  sync.Once
}

// NewMockStreamDefinitionBuilder creates a new builder pre-populated with
// the same defaults used by SetupClient.
func NewMockStreamDefinitionBuilder(objectName types.NamespacedName) *MockStreamDefinitionBuilder {
	return &MockStreamDefinitionBuilder{
		definition: &testv2.MockStreamDefinition{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "streaming.sneaksanddata.com/v2",
				Kind:       "MockStreamDefinition",
			},
			ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace},
			Spec: testv2.MockStreamDefinitionSpec{
				Source:      "sourceA",
				Destination: "destinationB",
				ExecutionSettings: testv2.ExecutionSettings{
					LayoutVersion: "v1",
					Suspended:     true,
					StreamingBackend: testv2.StreamingBackend{
						BatchJobBackend: &testv2.BatchJobBackend{},
						CronJobBackend:  nil,
					},
				},
			},
		},
	}
}

// WithSuspendedSpec sets the Suspended flag on the execution settings.
func (b *MockStreamDefinitionBuilder) WithSuspendedSpec(spec bool) *MockStreamDefinitionBuilder {
	b.definition.Spec.ExecutionSettings.Suspended = spec
	return b
}

// WithPhase sets the status phase of the stream definition.
func (b *MockStreamDefinitionBuilder) WithPhase(phase stream.Phase) *MockStreamDefinitionBuilder {
	b.definition.Status.Phase = string(phase)
	return b
}

// WithName sets the name and namespace of the stream definition.
func (b *MockStreamDefinitionBuilder) WithName(n types.NamespacedName) *MockStreamDefinitionBuilder {
	b.definition.Name = n.Name
	b.definition.Namespace = n.Namespace
	return b
}

// WithSchedule configures the stream definition with a cron job backend using
// the provided schedule, clearing the batch job backend.
func (b *MockStreamDefinitionBuilder) WithSchedule(schedule string) *MockStreamDefinitionBuilder {
	if b.definition.Spec.ExecutionSettings.StreamingBackend.CronJobBackend == nil {
		b.definition.Spec.ExecutionSettings.StreamingBackend.CronJobBackend = &testv2.CronJobBackend{}
		b.definition.Spec.ExecutionSettings.StreamingBackend.BatchJobBackend = nil
	}
	b.definition.Spec.ExecutionSettings.StreamingBackend.CronJobBackend.Schedule = schedule
	return b
}

// WithNoBackend configures the stream definition with an empty backend,
// clearing both the batch job and cron job backends.
func (b *MockStreamDefinitionBuilder) WithNoBackend() *MockStreamDefinitionBuilder {
	b.definition.Spec.ExecutionSettings.StreamingBackend.BatchJobBackend = nil
	b.definition.Spec.ExecutionSettings.StreamingBackend.CronJobBackend = nil
	return b
}

// WithStreamingJobTemplateRef sets the job template reference for the batch job backend,
// initializing the batch job backend if it is currently nil.
func (b *MockStreamDefinitionBuilder) WithStreamingJobTemplateRef(name types.NamespacedName) *MockStreamDefinitionBuilder {
	if b.definition.Spec.ExecutionSettings.StreamingBackend.BatchJobBackend == nil {
		b.definition.Spec.ExecutionSettings.StreamingBackend.BatchJobBackend = &testv2.BatchJobBackend{}
		b.definition.Spec.ExecutionSettings.StreamingBackend.CronJobBackend = nil
	}
	b.definition.Spec.ExecutionSettings.StreamingBackend.BatchJobBackend.JobTemplateRef.Name = name.Name
	b.definition.Spec.ExecutionSettings.StreamingBackend.BatchJobBackend.JobTemplateRef.Namespace = name.Namespace
	return b
}

// WithScheduledJobTemplateRef sets the job template reference for the batch job backend,
// initializing the batch job backend if it is currently nil.
func (b *MockStreamDefinitionBuilder) WithScheduledJobTemplateRef(name types.NamespacedName) *MockStreamDefinitionBuilder {
	if b.definition.Spec.ExecutionSettings.StreamingBackend.CronJobBackend == nil {
		b.definition.Spec.ExecutionSettings.StreamingBackend.CronJobBackend = &testv2.CronJobBackend{}
		b.definition.Spec.ExecutionSettings.StreamingBackend.BatchJobBackend = nil
	}
	b.definition.Spec.ExecutionSettings.StreamingBackend.CronJobBackend.JobTemplateRef.Name = name.Name
	b.definition.Spec.ExecutionSettings.StreamingBackend.CronJobBackend.JobTemplateRef.Namespace = name.Namespace
	return b
}

// WithBackfillJobTemplateRef sets the job template reference for the batch job backend,
// initializing the batch job backend if it is currently nil.
func (b *MockStreamDefinitionBuilder) WithBackfillJobTemplateRef(name types.NamespacedName) *MockStreamDefinitionBuilder {
	if b.definition.Spec.ExecutionSettings.StreamingBackend.BatchJobBackend == nil {
		b.definition.Spec.ExecutionSettings.StreamingBackend.BatchJobBackend = &testv2.BatchJobBackend{}
		b.definition.Spec.ExecutionSettings.StreamingBackend.CronJobBackend = nil
	}
	b.definition.Spec.ExecutionSettings.StreamingBackend.BatchJobBackend.BackfillJobTemplateRef.Name = name.Name
	b.definition.Spec.ExecutionSettings.StreamingBackend.BatchJobBackend.BackfillJobTemplateRef.Namespace = name.Namespace
	return b
}

// Apply runs an arbitrary mutation function on the underlying definition.
// This allows composing the builder with the existing functional-option style
// helpers in this package.
func (b *MockStreamDefinitionBuilder) Apply(fn func(definition *testv2.MockStreamDefinition)) *MockStreamDefinitionBuilder {
	if fn != nil {
		fn(b.definition)
	}
	return b
}

// Build returns the constructed *testv2.MockStreamDefinition. The result is
// computed on the first call and the same pointer is returned on subsequent
// calls.
func (b *MockStreamDefinitionBuilder) Build() *testv2.MockStreamDefinition {
	b.buildOnce.Do(func() {
		b.built = b.definition
	})
	return b.built
}
