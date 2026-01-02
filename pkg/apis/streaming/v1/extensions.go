package v1

import (
	"github.com/SneaksAndData/arcane-operator/services/job"
)

// BackfillRequest must implement the JobConfigurator interface to modify jobs for backfilling.
var _ job.JobConfigurator = &BackfillRequest{}

func (b *BackfillRequest) ConfigureJob(job *Job) error {
}
