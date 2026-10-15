package filtrify_test

import (
	"encoding/json"
	"testing"

	"github.com/liminaab/filtrify"
	"github.com/liminaab/filtrify/operatorv2"
	"github.com/liminaab/filtrify/test"
	"github.com/liminaab/filtrify/types"
	"github.com/stretchr/testify/assert"
)

func TestBasicGroupByv2(t *testing.T) {
	plainData, err := filtrify.ConvertToTypedDatav2(test.TestDataWithFields, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}
	conf := &operatorv2.GroupByConfiguration{
		GroupBy: []string{"gender", "country"},
		Select: []*operatorv2.AggregateSelect{
			{
				Columns: []string{"age"},
				Method:  "average",
			},
			{
				Columns: []string{"salary"},
				Method:  "sumx",
			},
		},
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.GroupBy,
		Configuration: string(b1),
	}

	groupedDatav2, err := filtrify.Transformv2(plainData, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "groupby column operation failed")
	}

	groupedData := rowsOf(t, groupedDatav2)
	for _, row := range groupedData.Rows {
		groupLevel := row.GetColumn(operatorv2.GroupLevelColName)
		assert.NotNil(t, groupLevel, "groupLevel column not found")
		level := groupLevel.CellValue.IntValue
		// check if the group by columns are present
		genderCol := row.GetColumn("gender")
		assert.NotNil(t, genderCol, "column not found")
		if level == 2 {
			countryCol := row.GetColumn("country")
			assert.NotNil(t, countryCol, "countryCol column not found")
		}

	}
}
