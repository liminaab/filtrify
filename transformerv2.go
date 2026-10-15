package filtrify

import (
	"errors"
	"fmt"

	"github.com/liminaab/filtrify/operatorv2"

	"github.com/liminaab/filtrify/types"
)

var injectedOperatorsv2 = make(map[types.TransformationOperatorType]types.TransformationOperatorv2)

func getOperatorv2(step *types.TransformationStep) (types.TransformationOperatorv2, error) {

	switch step.Operator {
	case types.Filter:
		return &operatorv2.FilterOperator{}, nil
	case types.NewColumn:
		return &operatorv2.NewColumnOperator{}, nil
	case types.Aggregate:
		return &operatorv2.AggregateOperator{}, nil
	case types.GroupBy:
		return &operatorv2.GroupByOperator{}, nil
	case types.Lookup:
		return &operatorv2.LookupOperator{}, nil
	case types.MappedValue:
		return &operatorv2.MappedValueOperator{}, nil
	case types.Sort:
		return &operatorv2.SortOperator{}, nil
	case types.RemoveColumn:
		return &operatorv2.RemoveColumnOperator{}, nil
	case types.RenameColumn:
		return &operatorv2.RenameColumnOperator{}, nil
	case types.ChangeColumnType:
		return &operatorv2.ChangeColumnTypeOperator{}, nil
	case types.JSON:
		return &operatorv2.JSONOperator{}, nil
	case types.CumulativeSum:
		return &operatorv2.CumulativeSumOperator{}, nil
	default:
		injectedOp, ok := injectedOperatorsv2[step.Operator]
		if !ok {
			return nil, fmt.Errorf("unknown operator %s", step.Operator)
		}
		return injectedOp, nil
	}

	return nil, errors.New("unknown operator")
}

func validateStepv2(step *types.TransformationStep) error {
	op, err := getOperatorv2(step)
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

func processTransformationv2(dataset *types.DataSetv2, step *types.TransformationStep, otherSets map[string]*types.DataSetv2) (*types.DataSetv2, error) {
	op, err := getOperatorv2(step)
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
	if dataset.NumRows() == 0 {
		return dataset, nil
	}
	transformedData, err := op.Transform(dataset, step.Configuration, otherSets)
	if err != nil {
		return nil, err
	}

	return transformedData, nil
}

func InjectOperatorv2(operatorCode types.TransformationOperatorType, operator types.TransformationOperatorv2) {
	injectedOperatorsv2[operatorCode] = operator
}

/*
func InjectSQLFunction(key string, op expr.CustomFunc) {
	lmnqlbridge.InjectFunction(key, op)
}
*/

func Transformv2(dataset *types.DataSetv2, transformations []*types.TransformationStep, otherSets map[string]*types.DataSetv2) (*types.DataSetv2, error) {
	newData := dataset
	var err error
	if dataset.NumRows() == 0 {
		return newData, nil
	}

	for i := 0; i < len(transformations); {
		ts := transformations[i]
		newData, err = processTransformationv2(newData, ts, otherSets)
		if err != nil {
			return nil, fmt.Errorf("could not apply transformation: %s (%s operator, step %d)", err.Error(), ts.Operator.String(), i)
		}
		i++
	}

	return newData, nil
}

func ValidateConfigurationv2(transformations []*types.TransformationStep) error {
	var err error
	for i, ts := range transformations {
		err = validateStepv2(ts)
		if err != nil {
			return fmt.Errorf("could not apply transformation: %s (%s operator, step %d)", err.Error(), ts.Operator.String(), i)
		}
	}

	return nil
}
