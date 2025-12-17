package v1

import (
	"github.com/SneaksAndData/arcane-operator/services/controllers/common"
)

var _ common.StreamClassWorkerIdentity = (*StreamClass)(nil)

func (in *StreamClass) WorkerId() common.WorkerId {
	return common.WorkerId(in.Name)
}
