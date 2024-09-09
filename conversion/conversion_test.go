package conversion

import (
	"github.com/liminaab/filtrify/types"
	assert2 "github.com/stretchr/testify/assert"
	"testing"
)

var sampleData = [][]string{
	{"Column1", "Column2", "Column3", "Column4", "Column5"},
	{"1", "2", "3", "4", "5"},
	{"6", "7", "8", "9", "10"},
	{"11", "12", "13", "14", "15"},
	{"16", "17", "18", "19", "20"},
}

func TestBasicConversion(t *testing.T) {
	assert := assert2.New(t)
	result, err := ConvertToTypedData(sampleData, true, true, nil, true)
	assert.Nil(err)
	assert.Equal(4, len(result.Rows))
	for i := 0; i < 4; i++ {
		row := result.Rows[i]
		assert.Equal(5, len(row.Columns))
		for j := 0; j < 5; j++ {
			assert.Equal(types.IntType, row.Columns[j].CellValue.DataType)
		}
	}
}

var sampleDateTable = [][]string{
	{"Date"},
	{"02/08/2024"},
	{"02/08/2024"},
	{"02/08/2024"},
	{"02/08/2024"},
	{"02/08/2024"},
	{"02/08/2024"},
	{"02/08/2024"},
	{"02/08/2024"},
	{"02/08/2024"},
	{"02/08/2024"},
	{"02/08/2024"},
}

func TestDateConversion(t *testing.T) {
	assert := assert2.New(t)
	result, err := ConvertToTypedData(sampleDateTable, true, true, nil, true)
	assert.Nil(err)
	assert.Equal(len(sampleDateTable)-1, len(result.Rows))
	for i := 0; i < len(sampleDateTable)-1; i++ {
		row := result.Rows[i]
		assert.Equal(1, len(row.Columns))
		col := row.Columns[0]
		// those rows can't be parsed as date - because it is not clear which is month and which is day
		assert.Equal(types.StringType, col.CellValue.DataType)
	}
}

var sampleDateTable2 = [][]string{
	{"Date"},
	{"02/13/2024"},
	{"02/13/2024"},
	{"02/13/2024"},
	{"02/13/2024"},
	{"02/13/2024"},
	{"02/13/2024"},
	{"02/13/2024"},
	{"02/13/2024"},
	{"02/13/2024"},
	{"02/13/2024"},
	{"02/13/2024"},
}

func TestDateConversion2(t *testing.T) {
	assert := assert2.New(t)
	result, err := ConvertToTypedData(sampleDateTable2, true, true, nil, true)
	assert.Nil(err)
	assert.Equal(len(sampleDateTable)-1, len(result.Rows))
	for i := 0; i < len(sampleDateTable)-1; i++ {
		row := result.Rows[i]
		assert.Equal(1, len(row.Columns))
		col := row.Columns[0]
		assert.Equal(types.DateType, col.CellValue.DataType)
		assert.Equal("02/13/2024", col.CellValue.TimestampValue.Format("01/02/2006"))
	}
}
