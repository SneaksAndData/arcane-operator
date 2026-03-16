package contracts

import (
	"fmt"

	"github.com/SneaksAndData/arcane-operator/services/controllers/contracts/v0"
	"github.com/SneaksAndData/arcane-operator/services/controllers/contracts/v1"
	"github.com/SneaksAndData/arcane-operator/services/controllers/stream"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// FromUnstructured converts an unstructured Kubernetes object into a stream.Definition, which is a structured
// representation of the stream definition.
func FromUnstructured(obj *unstructured.Unstructured) (stream.Definition, error) {

	apiVersion, found, err := unstructured.NestedString(obj.Object, "spec", "execution", "apiVersion")
	if err != nil { // coverage-ignore
		return nil, fmt.Errorf("error accessing apiVersion field: %w", err)
	}

	var v stream.Definition

	switch {
	case !found || apiVersion == "":
		v = v0.NewUnstructuredWrapper(obj)
	case apiVersion == "v1":
		v = v1.NewExecutionSettings(obj)
	default:
		return nil, fmt.Errorf("unknown apiVersion: %s", apiVersion)
	}

	err = v.Validate()
	if err != nil { // coverage-ignore
		return nil, fmt.Errorf("failed to parse Stream definition: %w", err)
	}
	return v, nil
}
