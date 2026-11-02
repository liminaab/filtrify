package operatorv2

import (
	"encoding/json"
	"errors"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/liminaab/filtrify/types"
)

type CumulativeSumOperator struct {
}

type CumulativeSumConfiguration struct {
	Column        string `json:"column"`
	NewColumnName string `json:"newColumnName"`
}

func (t *CumulativeSumOperator) Transform(dataset *types.DataSetv2, config string, _ map[string]*types.DataSetv2) (*types.DataSetv2, error) {

	typedConfig, err := t.buildConfiguration(config)
	if err != nil {
		return nil, err
	}

	theColumn := dataset.GetColumn(typedConfig.Column)
	if theColumn == nil {
		return nil, buildColumnNotExistsError(typedConfig.Column)
	}

	// A non-numeric column has nothing to add up, so every row carries the
	// untouched starting total - v1 reaches the same place by never finding a
	// numeric cell to accumulate.
	if !dataset.IsNumericType(theColumn.DataType()) {
		return dataset.AddConstantColumn(typedConfig.NewColumnName, float64(0), &arrow.Float64Type{})
	}

	// The new column is the running total *as of each row*, not the one grand
	// total repeated - that is what makes it cumulative. A null row adds nothing
	// but still carries the total so far, which is how v1 handles a cell that is
	// not numeric.
	builder := array.NewFloat64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.Reserve(int(dataset.NumRows()))

	var cumulativeSum float64 = 0
	for val := range dataset.NumericValues(theColumn) {
		if val != nil {
			cumulativeSum += *val
		}
		builder.Append(cumulativeSum)
	}

	arr := builder.NewArray()
	defer arr.Release()

	field := arrow.Field{Name: typedConfig.NewColumnName, Type: arrow.PrimitiveTypes.Float64, Nullable: true}
	chunked := arrow.NewChunked(field.Type, []arrow.Array{arr}) // retains arr
	defer chunked.Release()
	col := arrow.NewColumn(field, chunked) // retains chunked
	defer col.Release()

	return dataset.AddColumns(col)
}

func (t *CumulativeSumOperator) buildConfiguration(config string) (*CumulativeSumConfiguration, error) {
	if len(config) < 1 {
		return nil, errors.New("invalid configuration")
	}
	// config is a json declaration of our field configuration
	typedConfig := CumulativeSumConfiguration{}
	err := json.Unmarshal([]byte(config), &typedConfig)
	if err != nil {
		return nil, err
	}

	if len(typedConfig.Column) < 1 {
		return nil, errors.New("missing column name in CumulativeSum configuration")
	}
	if len(typedConfig.NewColumnName) < 1 {
		return nil, errors.New("missing new column name in CumulativeSum configuration")
	}

	return &typedConfig, nil
}

func (t *CumulativeSumOperator) ValidateConfiguration(config string) (bool, error) {
	typedConfig, err := t.buildConfiguration(config)
	return typedConfig != nil, err
}
