package filtrify_test

import (
	"encoding/json"
	"testing"

	"github.com/liminaab/filtrify"
	"github.com/liminaab/filtrify/operatorv2"
	"github.com/liminaab/filtrify/types"
	"github.com/stretchr/testify/assert"
)

func TestFilterInvalidColumn2v2(t *testing.T) {
	plainData, err := filtrify.ConvertToTypedDatav2(SEQTestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	filterConf := &operatorv2.FilterConfiguration{
		FilterCriteria: &operatorv2.FilterCriteria{
			Criteria: &operatorv2.Criteria{
				FieldName: "Instrument Class",
				Operator:  "!=",
				Value:     "Index Future",
			},
		},
	}

	aggregateConf := &operatorv2.AggregateConfiguration{
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

	_, err = filtrify.Transformv2(plainData, steps, nil)

	assert.Error(t, err, "invalid column on filter operation didn't return an error")
}
