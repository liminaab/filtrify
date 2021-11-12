package filtrify_test

import (
	"encoding/json"
	"testing"

	"github.com/liminaab/filtrify"
	"github.com/liminaab/filtrify/operator"
	"github.com/liminaab/filtrify/types"
	"github.com/stretchr/testify/assert"
)

func TestFilterInvalidColumn2(t *testing.T) {
	plainData, err := filtrify.ConvertToTypedData(SEQTestDataFormatted, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterConf := &operator.FilterConfiguration{
		FilterCriteria: &operator.FilterCriteria{
			Criteria: &operator.Criteria{
				FieldName: "Instrument Class",
				Operator:  "!=",
				Value:     "Index Future",
			},
		},
	}

	aggregateConf := &operator.AggregateConfiguration{
		GroupBy: []string{"Instrument Type"},
	}

	newColConfig := "{\"statement\": \"IFEL(`Market Value (Base)` > 5000000, 'Large', 'Small') AS `Size` \"}"

	filterConfText, err := json.Marshal(filterConf)
	if err != nil {
		panic(err.Error())
	}
	aggregateConfText, err := json.Marshal(aggregateConf)
	if err != nil {
		panic(err.Error())
	}
	steps := []*types.TransformationStep{
		{
			Operator:      types.Filter,
			Configuration: string(filterConfText),
		},
		{
			Operator:      types.Aggregate,
			Configuration: string(aggregateConfText),
		},
		{
			Operator:      types.NewColumn,
			Configuration: newColConfig,
		},
	}

	_, err = filtrify.Transform(plainData, steps, nil)

	assert.Error(t, err, "invalid column on filter operation didn't return an error")
}
