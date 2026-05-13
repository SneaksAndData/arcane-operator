package helpers

import (
	"context"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// GetStreamDefinitionUnstructured reads the MockStreamDefinition identified by
// name from the provided client and returns it as an *unstructured.Unstructured.
func GetStreamDefinitionUnstructured(ctx context.Context, k8sClient client.Client, name types.NamespacedName) (*unstructured.Unstructured, error) {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "streaming.sneaksanddata.com",
		Version: "v2",
		Kind:    "MockStreamDefinition",
	})
	if err := k8sClient.Get(ctx, name, u); err != nil {
		return nil, err
	}
	return u, nil
}
