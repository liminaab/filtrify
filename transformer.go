package dyntransformer

import (
	"errors"

	"limina.com/dyntransformer/operator"
	"limina.com/dyntransformer/types"
)

func processTransformation(dataset *types.DataSet, step *types.TransformationStep) (*types.DataSet, error) {
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
	default:
		return nil, errors.New("unknown operator")
	}

	// TODO check errors
	state, err := op.ValidateConfiguration(step.Configuration)
	if err != nil {
		return nil, err
	}
	if !state {
		return nil, errors.New("invalid configuration")
	}

	transformedData, err := op.Transform(dataset, step.Configuration)
	if err != nil {
		return nil, err
	}

	return transformedData, nil
}

func Transform(dataset *types.DataSet, transformations []*types.TransformationStep) (*types.DataSet, error) {
	newData := dataset
	var err error
	for _, ts := range transformations {
		newData, err = processTransformation(newData, ts)
		if err != nil {
			// wow we failed
			return nil, err
		}
	}

	return newData, nil
}
