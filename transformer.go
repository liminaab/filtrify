package filtrify

import (
	"errors"
	"fmt"

	"github.com/liminaab/filtrify/operator"
	"github.com/liminaab/filtrify/types"
)

func processTransformation(dataset *types.DataSet, step *types.TransformationStep, otherSets map[string]*types.DataSet) (*types.DataSet, error) {
	var op types.TransformationOperator

	switch step.Operator {
	case types.Filter:
		op = &operator.FilterOperator{}
		break
	case types.NewColumn:
		op = &operator.NewColumnOperator{}
		break
	case types.Aggregate:
		op = &operator.AggregateOperator{}
		break
	case types.Lookup:
		op = &operator.LookupOperator{}
		break
	case types.MappedValue:
		op = &operator.MappedValueOperator{}
		break
	case types.Sort:
		op = &operator.SortOperator{}
		break
	default:
		return nil, errors.New("unknown operator")
	}

	state, err := op.ValidateConfiguration(step.Configuration)
	if err != nil {
		return nil, err
	}
	if !state {
		return nil, errors.New("invalid configuration")
	}

	transformedData, err := op.Transform(dataset, step.Configuration, otherSets)
	if err != nil {
		return nil, err
	}

	return transformedData, nil
}

func Transform(dataset *types.DataSet, transformations []*types.TransformationStep, otherSets map[string]*types.DataSet) (*types.DataSet, error) {
	newData := dataset
	var err error
	for i, ts := range transformations {
		newData, err = processTransformation(newData, ts, otherSets)
		// let's wrap this error message to give more details
		if err != nil {
			// wow we failed
			return nil, fmt.Errorf("could not apply transformation: %s (%s operator, step %d)", err.Error(), ts.Operator.String(), i)
		}
	}

	return newData, nil
}
