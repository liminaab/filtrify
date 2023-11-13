package filtrify

import (
	"errors"
	"fmt"
	"github.com/araddon/qlbridge/expr"
	"github.com/liminaab/filtrify/lmnqlbridge"

	"github.com/liminaab/filtrify/operator"
	"github.com/liminaab/filtrify/types"
)

var injectedOperators = make(map[types.TransformationOperatorType]types.TransformationOperator)

func getOperator(step *types.TransformationStep) (types.TransformationOperator, error) {

	switch step.Operator {
	case types.Filter:
		return &operator.FilterOperator{}, nil
	case types.NewColumn:
		return &operator.NewColumnOperator{}, nil
	case types.Aggregate:
		return &operator.AggregateOperator{}, nil
	case types.Lookup:
		return &operator.LookupOperator{}, nil
	case types.MappedValue:
		return &operator.MappedValueOperator{}, nil
	case types.Sort:
		return &operator.SortOperator{}, nil
	case types.RemoveColumn:
		return &operator.RemoveColumnOperator{}, nil
	case types.RenameColumn:
		return &operator.RenameColumnOperator{}, nil
	case types.ChangeColumnType:
		return &operator.ChangeColumnTypeOperator{}, nil
	case types.JSON:
		return &operator.JSONOperator{}, nil
	case types.Objectify:
		return &operator.ObjectifyOperator{}, nil
	case types.CumulativeSum:
		return &operator.CumulativeSumOperator{}, nil
	default:
		operator, ok := injectedOperators[step.Operator]
		if !ok {
			return nil, fmt.Errorf("unknown operator %s", step.Operator)
		}
		return operator, nil
	}

	return nil, errors.New("unknown operator")
}

func validateStep(step *types.TransformationStep) error {
	op, err := getOperator(step)
	if err != nil {
		return err
	}
	state, err := op.ValidateConfiguration(step.Configuration)
	if err != nil {
		return err
	}
	if !state {
		return errors.New("invalid configuration")
	}

	return nil
}

func processTransformation(dataset *types.DataSet, step *types.TransformationStep, otherSets map[string]*types.DataSet) (*types.DataSet, error) {
	op, err := getOperator(step)
	if err != nil {
		return nil, err
	}
	state, err := op.ValidateConfiguration(step.Configuration)
	if err != nil {
		return nil, err
	}
	if !state {
		return nil, errors.New("invalid configuration")
	}
	if len(dataset.Rows) == 0 {
		return dataset, nil
	}
	transformedData, err := op.Transform(dataset, step.Configuration, otherSets)
	if err != nil {
		return nil, err
	}

	return transformedData, nil
}

func InjectOperator(operatorCode types.TransformationOperatorType, operator types.TransformationOperator) {
	injectedOperators[operatorCode] = operator
}

func InjectSQLFunction(key string, op expr.CustomFunc) {
	lmnqlbridge.InjectFunction(key, op)
}

func Transform(dataset *types.DataSet, transformations []*types.TransformationStep, otherSets map[string]*types.DataSet) (*types.DataSet, error) {
	newData := dataset
	var err error
	if len(dataset.Rows) == 0 {
		return newData, nil
	}

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

func ValidateConfiguration(transformations []*types.TransformationStep) error {
	var err error
	for i, ts := range transformations {
		err = validateStep(ts)
		// let's wrap this error message to give more details
		if err != nil {
			// wow we failed
			return fmt.Errorf("could not apply transformation: %s (%s operator, step %d)", err.Error(), ts.Operator.String(), i)
		}
	}

	return nil
}
