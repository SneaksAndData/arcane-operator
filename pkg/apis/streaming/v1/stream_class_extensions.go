package v1

import (
	"fmt"
	"github.com/SneaksAndData/arcane-operator/services/controllers/common"
)

var _ common.StreamClassWorkerIdentity = (*StreamClass)(nil)

func (in *StreamClass) WorkerId() common.WorkerId {
	return common.WorkerId(fmt.Sprintf("%s-%s", in.Namespace, in.Name))
}
