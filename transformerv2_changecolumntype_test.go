package filtrify_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/liminaab/filtrify"
	"github.com/liminaab/filtrify/conversion"
	"github.com/liminaab/filtrify/operatorv2"
	"github.com/liminaab/filtrify/test"
	"github.com/liminaab/filtrify/types"
	"github.com/stretchr/testify/assert"
)

func TestSkipConversionIfFailsv2(t *testing.T) {
	datav2, err := filtrify.ConvertToTypedDatav2(TestData2, true, true, false)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	tr := true
	conf := &operatorv2.ChangeColumnTypeConfiguration{
		Columns: map[string]operatorv2.ConversionConfiguration{
			"a": {
				TargetType: types.LongType,
			},
			"b": {
				TargetType:            types.LongType,
				SkipConversionIfFails: &tr,
			},
		},
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: string(b1),
	}

	data := rowsOf(t, datav2)
	firstCol := test.GetColumn(data.Rows[0], "a")
	assert.NotNil(t, firstCol, fmt.Sprintf("%s column was not found", "Quantity"))
	if firstCol.CellValue.DataType != types.StringType {
		assert.Fail(t, "Type conversion init failed")
	}

	sortedDatav2, err := filtrify.Transformv2(datav2, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "convert column operation failed")
	}

	sortedData := rowsOf(t, sortedDatav2)
	for i, row := range sortedData.Rows {
		firstCol = test.GetColumn(row, "b")
		assert.NotNil(t, firstCol, fmt.Sprintf("%s column was not found", "b"))
		// according to the new code - it should only set last row as nil type
		if i == len(sortedData.Rows)-1 {
			if firstCol.CellValue.DataType != types.NilType {
				assert.Fail(t, "Type conversion failed")
			}
		} else {
			if firstCol.CellValue.DataType != types.LongType {
				assert.Fail(t, "Type conversion failed")
			}
		}

		firstCol = test.GetColumn(row, "a")
		assert.NotNil(t, firstCol, fmt.Sprintf("%s column was not found", "b"))
		if firstCol.CellValue.DataType != types.LongType {
			assert.Fail(t, "Type conversion failed")
		}
	}
}

func TestChangeColumnTypev2(t *testing.T) {
	data, err := filtrify.ConvertToTypedDatav2(test.UAT1TestDataFormatted, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	changeColumnType := types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: `{"columns":{"Quantity":{"targetType":4,"stringNumericConfiguration":{"decimalSymbol":".","thousandSeperator":"","numberOfDecimals":0}}}}`,
	}
	plainDataConvertedv2, err := filtrify.Transformv2(data, []*types.TransformationStep{&changeColumnType}, nil)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	conf := &operatorv2.ChangeColumnTypeConfiguration{
		Columns: map[string]operatorv2.ConversionConfiguration{
			"Quantity": {
				TargetType: types.StringType,
			},
		},
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: string(b1),
	}

	plainDataConverted := rowsOf(t, plainDataConvertedv2)
	firstCol := test.GetColumn(plainDataConverted.Rows[0], "Quantity")
	assert.NotNil(t, firstCol, fmt.Sprintf("%s column was not found", "Quantity"))
	if firstCol.CellValue.DataType != types.DoubleType {
		assert.Fail(t, "Type conversion init failed")
	}

	sortedDatav2, err := filtrify.Transformv2(plainDataConvertedv2, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}

	sortedData := rowsOf(t, sortedDatav2)
	firstCol = test.GetColumn(sortedData.Rows[0], "Quantity")
	assert.NotNil(t, firstCol, fmt.Sprintf("%s column was not found", "Quantity"))
	if firstCol.CellValue.DataType != types.StringType {
		assert.Fail(t, "Type conversion failed")
	}
}

func TestChangeColumnType2v2(t *testing.T) {
	datav2, err := filtrify.ConvertToTypedDatav2(TestData, true, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	conf := &operatorv2.ChangeColumnTypeConfiguration{
		Columns: map[string]operatorv2.ConversionConfiguration{
			"Quantity": {
				TargetType: types.DoubleType,
				StringNumeric: &operatorv2.StringNumericConfiguration{
					DecimalSymbol:     ".",
					ThousandSeperator: ",",
				},
			},
		},
	}
	b1, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	step := &types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: string(b1),
	}

	data := rowsOf(t, datav2)
	firstCol := test.GetColumn(data.Rows[0], "Quantity")
	assert.NotNil(t, firstCol, fmt.Sprintf("%s column was not found", "Quantity"))
	if firstCol.CellValue.DataType != types.StringType {
		assert.Fail(t, "Type conversion init failed")
	}

	sortedDatav2, err := filtrify.Transformv2(datav2, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}

	sortedData := rowsOf(t, sortedDatav2)
	firstCol = test.GetColumn(sortedData.Rows[0], "Quantity")
	assert.NotNil(t, firstCol, fmt.Sprintf("%s column was not found", "Quantity"))
	if firstCol.CellValue.DataType != types.DoubleType {
		assert.Fail(t, "Type conversion failed")
	}
}

// getConfigForConversionColumnv2 is getConfigForConversionColumn against the v2
// configuration types. The two sets of types carry identical JSON tags, so the
// configuration text it produces is the same - what changes is only which package
// the test writes the literal in.
func getConfigForConversionColumnv2(conf operatorv2.ConversionConfiguration) string {
	c := &operatorv2.ChangeColumnTypeConfiguration{
		Columns: map[string]operatorv2.ConversionConfiguration{
			"col1": conf,
		},
	}

	b1, err := json.Marshal(c)
	if err != nil {
		panic(err.Error())
	}
	return string(b1)
}

// v2WantOverrides lists the cases of TestChangeColumnTypeCombinationsv2 where the
// v2 result cannot equal the v1 one, because the INPUT cell loses information on
// the way into arrow - before the operator runs at all. Two limits account for
// every entry:
//
//   - A date is an arrow Date32: days since the epoch, with no clock. v1 holds a
//     DateType cell as a full time.Time, and these cases hand one in with a
//     16:45:09 or 08:35:44 clock that v1's converters then read. In v2 that clock
//     is already gone, so the date converts from midnight.
//   - A time of day is an arrow Time64us: microseconds since midnight, no date and
//     no nanoseconds. v1 holds one as a time.Time, so its expected value carries
//     both a date (-0001-11-30, what time.Date(0, 0, 0, ...) normalises to) and
//     533ns. In v2 the clock survives to the microsecond and the date is rebuilt
//     as year 0 January 1 by rowsOf.
//
// Every override below is the same conversion as v1 applied to the value arrow can
// actually hold; the clock or the date, whichever the case is about, still matches.
// Nothing here is the operator computing a different answer from the same input -
// where the input survives, v2 agrees with v1 exactly, which is the other ~60
// cases of this table.
var v2WantOverrides = map[string]interface{}{
	// Date32 has no clock: 2023-03-28 08:35:44 arrives as 2023-03-28 00:00:00, so
	// the unix encodings are midnight's rather than 08:35:44's.
	"date_to_long_2_unix_millis":  int64(1679961600000),
	"date_to_double_unix_seconds": float64(1679961600),
	"date_to_string_4":            "18.03.2021 00:00:00",
	"date_to_string_5":            "18.03.2021 00:00:00",
	// Time64us has no date and no sub-microsecond part: the clock is the v1 one,
	// the 533ns is not representable and the date is rowsOf's year 0 January 1.
	"datetime_to_time":       time.Date(0, 1, 1, 8, 35, 44, 0, time.UTC),
	"datetime_to_time_CET":   time.Date(0, 1, 1, 10, 35, 44, 0, time.UTC),
	"datetime_to_time_CET_2": time.Date(0, 1, 1, 9, 35, 44, 0, time.UTC),
}

// TestChangeColumnTypeCombinationsv2 is v1's TestChangeColumnTypeCombinations run
// against the v2 operator. The cases - every from-type/to-type pair with its
// expected value - are the v1 ones, taken from the v1 table builders verbatim; the
// single-cell v1 dataset each case carries is migrated to v2 with
// conversion.MigrateToV2, and the result is read back with rowsOf so the
// assertion stays `converted.Rows[0].Columns[0].CellValue.Value()`. The expected
// value is the v1 one except for the cases v2WantOverrides documents.
func TestChangeColumnTypeCombinationsv2(t *testing.T) {
	tests := []conversionTest{}
	tests = append(tests, getDateTimeConversionTests()...)
	tests = append(tests, getDateConversionTests()...)
	tests = append(tests, getDoubleConversionTests()...)
	tests = append(tests, getLongConversionTests()...)
	tests = append(tests, getIntConversionTests()...)
	tests = append(tests, getTimeConversionTests()...)
	tests = append(tests, getStringConversionTests()...)

	changeColumnTypeOp := operatorv2.ChangeColumnTypeOperator{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)
			source := tt.data
			dsv2, err := conversion.MigrateToV2(&source)
			assert.Nil(err, "migrating the case dataset to v2 failed")

			want := tt.want
			if override, found := v2WantOverrides[tt.name]; found {
				want = override
			}

			convertedv2, err := changeColumnTypeOp.Transform(dsv2, tt.config, nil)
			assert.Nil(err, "Column type conversion failed")
			converted := rowsOf(t, convertedv2)
			assert.Equal(want, converted.Rows[0].Columns[0].CellValue.Value(), "Column type conversion failed")
		})
	}
}

// See the note on TestRemoveColumnv2WithRowKey: a v2 dataset has no per-row key,
// so what is left of v1's TestChangeColumnTypeWithRowKey is
// TestChangeColumnTypev2.
func TestChangeColumnTypev2WithRowKey(t *testing.T) {
	t.Skip("v2 datasets carry no per-row key - see rowsOf in transformerv2_common_test.go")
}
