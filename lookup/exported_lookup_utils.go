package lookup

import (
	"github.com/liminaab/filtrify/operator"
	"github.com/liminaab/filtrify/types"
)

type Configuration struct {
	JoinColumns              []*operator.JoinColumn
	SelectRightColumns       []string
	RightPrefix              string
	RemoveRightMatchColumn   bool
	RemoveRightDatasetPrefix bool
}

type JoinColumn = operator.JoinColumn

func LeftJoin(leftSet *types.DataSet, configuration Configuration, rightSet *types.DataSet) (*types.DataSet, error) {
	op := operator.LookupOperator{}
	return op.TransformTyped(leftSet, &operator.LookupConfiguration{
		TargetDataset:            configuration.RightPrefix,
		Columns:                  configuration.JoinColumns,
		RemoveRightMatchColumn:   configuration.RemoveRightMatchColumn,
		RemoveRightDatasetPrefix: configuration.RemoveRightDatasetPrefix,
		SelectedColumns:          configuration.SelectRightColumns,
	}, map[string]*types.DataSet{configuration.RightPrefix: rightSet})
}
