package v1

import (
	"sync"

	testv1 "github.com/SneaksAndData/arcane-operator/pkg/test/apis_test/streaming/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// MockStreamDefinitionBuilder provides a fluent builder for constructing
// *testv1.MockStreamDefinition objects in tests.
type MockStreamDefinitionBuilder struct {
	definition *testv1.MockStreamDefinition
	built      *testv1.MockStreamDefinition
	buildOnce  sync.Once
}

// NewMockStreamDefinitionBuilder creates a new builder pre-populated with
// defaults used by the v1 test setup.
func NewMockStreamDefinitionBuilder(objectName types.NamespacedName) *MockStreamDefinitionBuilder {
	return &MockStreamDefinitionBuilder{
		definition: &testv1.MockStreamDefinition{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "streaming.sneaksanddata.com/v1",
				Kind:       "MockStreamDefinition",
			},
			ObjectMeta: metav1.ObjectMeta{Name: objectName.Name, Namespace: objectName.Namespace},
			Spec: testv1.MockStreamDefinitionSpec{
				Source:      "sourceA",
				Destination: "destinationB",
				Suspended:   true,
			},
		},
	}
}

// WithSuspendedSpec sets the Suspended flag on the spec.
func (b *MockStreamDefinitionBuilder) WithSuspendedSpec(spec bool) *MockStreamDefinitionBuilder {
	b.definition.Spec.Suspended = spec
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

// Apply runs an arbitrary mutation function on the underlying definition.
func (b *MockStreamDefinitionBuilder) Apply(fn func(definition *testv1.MockStreamDefinition)) *MockStreamDefinitionBuilder {
	if fn != nil {
		fn(b.definition)
	}
	return b
}

// Build returns the constructed *testv1.MockStreamDefinition. The result is
// computed on the first call and the same pointer is returned on subsequent
// calls.
func (b *MockStreamDefinitionBuilder) Build() *testv1.MockStreamDefinition {
	b.buildOnce.Do(func() {
		b.built = b.definition
	})
	return b.built
}
