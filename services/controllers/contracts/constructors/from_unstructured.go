package constructors

import (
	"fmt"

	v0 "github.com/SneaksAndData/arcane-operator/services/controllers/contracts/v0"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func FromUnstructured(obj *unstructured.Unstructured) (stream.Definition, error) {

	v := v0.UnstructuredWrapper{
		Underlying: obj,
	}

	err := v.Validate()
	if err != nil { // coverage-ignore
		return nil, fmt.Errorf("failed to parse Stream definition: %w", err)
	}
	return &v, nil
}
