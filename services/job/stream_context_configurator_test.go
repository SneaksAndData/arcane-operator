package job

import (
	"github.com/stretchr/testify/require"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

func Test_StreamContextConfigurator_Set_EnvVar_Single_Container(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "test-container"}}

	baseObject := map[string]string{
		"source":      "test-source",
		"destination": "test-destination",
	}

	configurator := NewEnvironmentConfigurator(baseObject, "stream_config")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)

	container := job.Spec.Template.Spec.Containers[0]
	require.NotEmpty(t, container.Env)

	found := false
	for _, env := range container.Env {
		if env.Name == "STREAMCONTEXT__STREAM_CONFIG" {
			require.Contains(t, env.Value, "test-source")
			require.Contains(t, env.Value, "test-destination")
			found = true
			break
		}
	}
	require.True(t, found, "STREAMCONTEXT__STREAM_CONFIG should be set")
}

func Test_StreamContextConfigurator_Multiple_Containers(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{
		{Name: "container-1"},
		{Name: "container-2"},
		{Name: "container-3"},
	}

	baseObject := map[string]interface{}{
		"key": "value",
	}

	configurator := NewEnvironmentConfigurator(baseObject, "config")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)

	// Verify env var is set in all containers
	for i, container := range job.Spec.Template.Spec.Containers {
		require.NotEmpty(t, container.Env, "Container %d should have environment variables", i)

		found := false
		for _, env := range container.Env {
			if env.Name == "STREAMCONTEXT__CONFIG" {
				require.Contains(t, env.Value, "value")
				found = true
				break
			}
		}
		require.True(t, found, "STREAMCONTEXT__CONFIG should be set in container %d", i)
	}
}

func Test_StreamContextConfigurator_Key_Uppercase_Conversion(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "test-container"}}

	baseObject := "simple-string"

	configurator := NewEnvironmentConfigurator(baseObject, "lowercase_key")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)

	container := job.Spec.Template.Spec.Containers[0]
	found := false
	for _, env := range container.Env {
		if env.Name == "STREAMCONTEXT__LOWERCASE_KEY" {
			found = true
			break
		}
	}
	require.True(t, found, "Environment key should be converted to uppercase")
}

func Test_StreamContextConfigurator_Complex_Object(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "test-container"}}

	type ComplexConfig struct {
		Source      string            `json:"source"`
		Destination string            `json:"destination"`
		Metadata    map[string]string `json:"metadata"`
		Enabled     bool              `json:"enabled"`
	}

	baseObject := ComplexConfig{
		Source:      "kafka://localhost:9092",
		Destination: "s3://bucket/path",
		Metadata: map[string]string{
			"version": "1.0",
			"env":     "production",
		},
		Enabled: true,
	}

	configurator := NewEnvironmentConfigurator(baseObject, "stream_definition")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)

	container := job.Spec.Template.Spec.Containers[0]
	found := false
	for _, env := range container.Env {
		if env.Name == "STREAMCONTEXT__STREAM_DEFINITION" {
			require.Contains(t, env.Value, "kafka://localhost:9092")
			require.Contains(t, env.Value, "s3://bucket/path")
			require.Contains(t, env.Value, "version")
			require.Contains(t, env.Value, "production")
			found = true
			break
		}
	}
	require.True(t, found, "STREAMCONTEXT__STREAM_DEFINITION should be set with complex object")
}

func Test_StreamContextConfigurator_String_Value(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "test-container"}}

	baseObject := "simple-string-value"

	configurator := NewEnvironmentConfigurator(baseObject, "string_key")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)

	container := job.Spec.Template.Spec.Containers[0]
	found := false
	for _, env := range container.Env {
		if env.Name == "STREAMCONTEXT__STRING_KEY" {
			require.Equal(t, "\"simple-string-value\"", env.Value) // JSON marshaled string includes quotes
			found = true
			break
		}
	}
	require.True(t, found, "STREAMCONTEXT__STRING_KEY should be set")
}

func Test_StreamContextConfigurator_Numeric_Value(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "test-container"}}

	baseObject := 12345

	configurator := NewEnvironmentConfigurator(baseObject, "numeric_key")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)

	container := job.Spec.Template.Spec.Containers[0]
	found := false
	for _, env := range container.Env {
		if env.Name == "STREAMCONTEXT__NUMERIC_KEY" {
			require.Equal(t, "12345", env.Value)
			found = true
			break
		}
	}
	require.True(t, found, "STREAMCONTEXT__NUMERIC_KEY should be set")
}

func Test_StreamContextConfigurator_Boolean_Value(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "test-container"}}

	configurator := NewEnvironmentConfigurator(true, "boolean_key")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)

	container := job.Spec.Template.Spec.Containers[0]
	found := false
	for _, env := range container.Env {
		if env.Name == "STREAMCONTEXT__BOOLEAN_KEY" {
			require.Equal(t, "true", env.Value)
			found = true
			break
		}
	}
	require.True(t, found, "STREAMCONTEXT__BOOLEAN_KEY should be set")
}

func Test_StreamContextConfigurator_Array_Value(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "test-container"}}

	baseObject := []string{"item1", "item2", "item3"}

	configurator := NewEnvironmentConfigurator(baseObject, "array_key")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)

	container := job.Spec.Template.Spec.Containers[0]
	found := false
	for _, env := range container.Env {
		if env.Name == "STREAMCONTEXT__ARRAY_KEY" {
			require.Contains(t, env.Value, "item1")
			require.Contains(t, env.Value, "item2")
			require.Contains(t, env.Value, "item3")
			found = true
			break
		}
	}
	require.True(t, found, "STREAMCONTEXT__ARRAY_KEY should be set")
}

func Test_StreamContextConfigurator_Nil_Value(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "test-container"}}

	var baseObject interface{} = nil

	configurator := NewEnvironmentConfigurator(baseObject, "nil_key")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)

	container := job.Spec.Template.Spec.Containers[0]
	found := false
	for _, env := range container.Env {
		if env.Name == "STREAMCONTEXT__NIL_KEY" {
			require.Equal(t, "null", env.Value)
			found = true
			break
		}
	}
	require.True(t, found, "STREAMCONTEXT__NIL_KEY should be set")
}

func Test_StreamContextConfigurator_Empty_String_Key(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "test-container"}}

	baseObject := "test-value"

	configurator := NewEnvironmentConfigurator(baseObject, "")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)

	container := job.Spec.Template.Spec.Containers[0]
	found := false
	for _, env := range container.Env {
		if env.Name == "STREAMCONTEXT__" {
			found = true
			break
		}
	}
	require.True(t, found, "STREAMCONTEXT__ should be set even with empty key")
}

func Test_StreamContextConfigurator_Append_To_Existing_EnvVars(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{
		Name: "test-container",
		Env: []corev1.EnvVar{
			{Name: "EXISTING_VAR", Value: "existing-value"},
			{Name: "ANOTHER_VAR", Value: "another-value"},
		},
	}}

	baseObject := map[string]string{"key": "value"}

	configurator := NewEnvironmentConfigurator(baseObject, "new_config")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)

	container := job.Spec.Template.Spec.Containers[0]
	require.Len(t, container.Env, 3, "Should have 3 environment variables")

	// Verify existing vars are preserved
	foundExisting := false
	foundAnother := false
	foundNew := false
	for _, env := range container.Env {
		if env.Name == "EXISTING_VAR" {
			require.Equal(t, "existing-value", env.Value)
			foundExisting = true
		}
		if env.Name == "ANOTHER_VAR" {
			require.Equal(t, "another-value", env.Value)
			foundAnother = true
		}
		if env.Name == "STREAMCONTEXT__NEW_CONFIG" {
			foundNew = true
		}
	}
	require.True(t, foundExisting, "EXISTING_VAR should be preserved")
	require.True(t, foundAnother, "ANOTHER_VAR should be preserved")
	require.True(t, foundNew, "STREAMCONTEXT__NEW_CONFIG should be added")
}

func Test_StreamContextConfigurator_No_Containers(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{}

	baseObject := "test-value"

	configurator := NewEnvironmentConfigurator(baseObject, "test_key")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Empty(t, job.Spec.Template.Spec.Containers)
}

func Test_StreamContextConfigurator_Map_Value(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "test-container"}}

	baseObject := map[string]interface{}{
		"string": "value",
		"number": 42,
		"bool":   true,
		"nested": map[string]string{
			"inner": "data",
		},
	}

	configurator := NewEnvironmentConfigurator(baseObject, "map_key")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)

	container := job.Spec.Template.Spec.Containers[0]
	found := false
	for _, env := range container.Env {
		if env.Name == "STREAMCONTEXT__MAP_KEY" {
			require.Contains(t, env.Value, "value")
			require.Contains(t, env.Value, "42")
			require.Contains(t, env.Value, "true")
			require.Contains(t, env.Value, "inner")
			require.Contains(t, env.Value, "data")
			found = true
			break
		}
	}
	require.True(t, found, "STREAMCONTEXT__MAP_KEY should be set with map data")
}

func Test_StreamContextConfigurator_Special_Characters_In_Key(t *testing.T) {
	job := &batchv1.Job{}
	job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "test-container"}}

	baseObject := "test-value"

	configurator := NewEnvironmentConfigurator(baseObject, "key_with-special.chars")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)

	container := job.Spec.Template.Spec.Containers[0]
	found := false
	for _, env := range container.Env {
		if env.Name == "STREAMCONTEXT__KEY_WITH-SPECIAL.CHARS" {
			found = true
			break
		}
	}
	require.True(t, found, "Environment key should handle special characters")
}

func Test_StreamContextConfigurator_Does_Not_Affect_Other_Properties(t *testing.T) {
	job := &batchv1.Job{}
	job.Name = "test-job"
	job.Namespace = "test-namespace"
	job.Labels = map[string]string{"app": "test"}
	job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "test-container"}}

	baseObject := "test-value"

	configurator := NewEnvironmentConfigurator(baseObject, "test_key")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)

	require.Equal(t, "test-job", job.Name)
	require.Equal(t, "test-namespace", job.Namespace)
	require.Equal(t, "test", job.Labels["app"])
}
