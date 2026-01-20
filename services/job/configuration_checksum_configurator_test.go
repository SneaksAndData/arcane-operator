package job

import (
	"github.com/stretchr/testify/require"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
)

func Test_ConfigurationChecksumConfigurator_Annotations_Set(t *testing.T) {
	job := &batchv1.Job{}

	configurator := NewConfigurationChecksumConfigurator("abc123")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Contains(t, job.Annotations, ConfigurationHashAnnotation)
	require.Equal(t, "abc123", job.Annotations[ConfigurationHashAnnotation])
}

func Test_ConfigurationChecksumConfigurator_Annotations_EmptyChecksum(t *testing.T) {
	job := &batchv1.Job{}

	configurator := NewConfigurationChecksumConfigurator("")
	err := configurator.ConfigureJob(job)
	require.Error(t, err)
	require.EqualError(t, err, "configuration checksum cannot be empty")
	require.NotContains(t, job.Annotations, ConfigurationHashAnnotation)
}

func Test_ConfigurationChecksumConfigurator_Annotations_Update_Existing(t *testing.T) {
	job := &batchv1.Job{}
	job.Annotations = map[string]string{
		ConfigurationHashAnnotation: "oldHash",
	}

	configurator := NewConfigurationChecksumConfigurator("newHash123")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Contains(t, job.Annotations, ConfigurationHashAnnotation)
	require.Equal(t, "newHash123", job.Annotations[ConfigurationHashAnnotation])
}

func Test_ConfigurationChecksumConfigurator_Annotations_Nil_Map(t *testing.T) {
	job := &batchv1.Job{}
	job.Annotations = nil

	configurator := NewConfigurationChecksumConfigurator("hash456")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Contains(t, job.Annotations, ConfigurationHashAnnotation)
	require.Equal(t, "hash456", job.Annotations[ConfigurationHashAnnotation])
}

func Test_ConfigurationChecksumConfigurator_Annotations_Preserve_Other(t *testing.T) {
	job := &batchv1.Job{}
	job.Annotations = map[string]string{
		"other-annotation": "other-value",
		"custom-key":       "custom-value",
	}

	configurator := NewConfigurationChecksumConfigurator("xyz789")
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Contains(t, job.Annotations, ConfigurationHashAnnotation)
	require.Equal(t, "xyz789", job.Annotations[ConfigurationHashAnnotation])
	require.Equal(t, "other-value", job.Annotations["other-annotation"])
	require.Equal(t, "custom-value", job.Annotations["custom-key"])
	require.Len(t, job.Annotations, 3)
}

func Test_ConfigurationChecksumConfigurator_Annotations_LongHash(t *testing.T) {
	job := &batchv1.Job{}

	longHash := "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0u1v2w3x4y5z6"
	configurator := NewConfigurationChecksumConfigurator(longHash)
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Contains(t, job.Annotations, ConfigurationHashAnnotation)
	require.Equal(t, longHash, job.Annotations[ConfigurationHashAnnotation])
}

func Test_ConfigurationChecksumConfigurator_Annotations_SpecialCharacters(t *testing.T) {
	job := &batchv1.Job{}

	specialHash := "hash-with_special.chars@123!"
	configurator := NewConfigurationChecksumConfigurator(specialHash)
	err := configurator.ConfigureJob(job)
	require.NoError(t, err)
	require.Contains(t, job.Annotations, ConfigurationHashAnnotation)
	require.Equal(t, specialHash, job.Annotations[ConfigurationHashAnnotation])
}
