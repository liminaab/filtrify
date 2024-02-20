package filtrify_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/liminaab/filtrify"
	"github.com/liminaab/filtrify/operator"
	"github.com/liminaab/filtrify/test"
	"github.com/liminaab/filtrify/types"
	"github.com/stretchr/testify/assert"
)

var TestData [][]string = [][]string{
	{"Instrument name", "Instrument Type", "Quantity", "Market Value (Base)", "Exposure %", "Maturity Date", "EU Sanction listed", "Active From"},
	{"ERIC B SS Equity", "Equity", "175,000.00", "2000000.00", "8%", "", "true", "2020-01-01 12:00:00"},
	{"AMZN US Equity", "Equity", "1,500.00", "6000000.00", "25%", "", "false", "2020-03-01 12:00:00"},
	{"T 0 12/31/21", "Bill", "9,000,000.00", "8750000.00", "30%", "2021-12-31", "false", "2020-11-22 12:00:00"},
	{"ESZ1", "Index Future", "-10.00", "-495000.00", "17%", "2021-12-16", "false", "2021-04-06 12:00:00"},
	{"USD Cash", "Cash Account", "5,000,000.00", "5000000.0", "20%", "", "", "2020-01-01 12:00:00"},
}

func TestChangeColumnType(t *testing.T) {
	data, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	changeColumnType := types.TransformationStep{
		Operator:      types.ChangeColumnType,
		Configuration: `{"columns":{"Quantity":{"targetType":4,"stringNumericConfiguration":{"decimalSymbol":".","thousandSeperator":"","numberOfDecimals":0}}}}`,
	}
	plainDataConverted, err := filtrify.Transform(data, []*types.TransformationStep{&changeColumnType}, nil)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	conf := &operator.ChangeColumnTypeConfiguration{
		Columns: map[string]operator.ConversionConfiguration{
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

	firstCol := test.GetColumn(plainDataConverted.Rows[0], "Quantity")
	assert.NotNil(t, firstCol, fmt.Sprintf("%s column was not found", "Quantity"))
	if firstCol.CellValue.DataType != types.DoubleType {
		assert.Fail(t, "Type conversion init failed")
	}

	sortedData, err := filtrify.Transform(plainDataConverted, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}

	firstCol = test.GetColumn(sortedData.Rows[0], "Quantity")
	assert.NotNil(t, firstCol, fmt.Sprintf("%s column was not found", "Quantity"))
	if firstCol.CellValue.DataType != types.StringType {
		assert.Fail(t, "Type conversion failed")
	}
}

func TestChangeColumnType2(t *testing.T) {
	data, err := filtrify.ConvertToTypedData(TestData, true, true)
	if err != nil {
		assert.NoError(t, err, "basic data conversion failed")
	}

	conf := &operator.ChangeColumnTypeConfiguration{
		Columns: map[string]operator.ConversionConfiguration{
			"Quantity": {
				TargetType: types.DoubleType,
				StringNumeric: &operator.StringNumericConfiguration{
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

	firstCol := test.GetColumn(data.Rows[0], "Quantity")
	assert.NotNil(t, firstCol, fmt.Sprintf("%s column was not found", "Quantity"))
	if firstCol.CellValue.DataType != types.StringType {
		assert.Fail(t, "Type conversion init failed")
	}

	sortedData, err := filtrify.Transform(data, []*types.TransformationStep{step}, nil)
	if err != nil {
		assert.NoError(t, err, "new aggregation column operation failed")
	}

	firstCol = test.GetColumn(sortedData.Rows[0], "Quantity")
	assert.NotNil(t, firstCol, fmt.Sprintf("%s column was not found", "Quantity"))
	if firstCol.CellValue.DataType != types.DoubleType {
		assert.Fail(t, "Type conversion failed")
	}
}

func getDatasetForConversion(val types.CellValue) types.DataSet {
	return types.DataSet{
		Rows: []*types.DataRow{
			{
				Columns: []*types.DataColumn{
					{
						ColumnName: "col1",
						CellValue:  &val,
					},
				},
			},
		},
	}
}

func getConfigForConversionColumn(conf operator.ConversionConfiguration) string {
	c := &operator.ChangeColumnTypeConfiguration{
		Columns: map[string]operator.ConversionConfiguration{
			"col1": conf,
		},
	}

	b1, err := json.Marshal(c)
	if err != nil {
		panic(err.Error())
	}
	return string(b1)
}

type conversionTest struct {
	name   string
	data   types.DataSet
	want   interface{}
	config string
}

func getDateTimeConversionTests() []conversionTest {
	return []conversionTest{
		{
			name:   "datetime_to_string_1",
			data:   getDatasetForConversion(types.CellValue{DataType: types.TimestampType, TimestampValue: time.Date(2021, 3, 18, 16, 45, 9, 0, time.UTC)}),
			want:   "2021-03-18 16:45:09",
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.StringType, StringDate: &operator.StringDateConfiguration{DateFormat: "yyyy-MM-dd hh:mm:ss"}}),
		},
		{
			name:   "datetime_to_string_2",
			data:   getDatasetForConversion(types.CellValue{DataType: types.TimestampType, TimestampValue: time.Date(2021, 3, 18, 16, 45, 9, 0, time.UTC)}),
			want:   "2021-03-18",
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.StringType, StringDate: &operator.StringDateConfiguration{DateFormat: "yyyy-MM-dd"}}),
		},
		{
			name:   "datetime_to_string_3",
			data:   getDatasetForConversion(types.CellValue{DataType: types.TimestampType, TimestampValue: time.Date(2021, 3, 18, 16, 45, 9, 0, time.UTC)}),
			want:   "16:45:09",
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.StringType, StringDate: &operator.StringDateConfiguration{DateFormat: "hh:mm:ss"}}),
		},
		{
			name:   "datetime_to_int_excel_date",
			data:   getDatasetForConversion(types.CellValue{DataType: types.TimestampType, TimestampValue: time.Date(2015, 1, 1, 16, 45, 9, 0, time.UTC)}),
			want:   int32(42005),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.IntType, NumericDate: &operator.NumericDateConfiguration{IsExcelDate: true}}),
		},
		{
			name:   "datetime_to_long_excel_date",
			data:   getDatasetForConversion(types.CellValue{DataType: types.TimestampType, TimestampValue: time.Date(2015, 1, 2, 16, 45, 9, 0, time.UTC)}),
			want:   int64(42006),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.LongType, NumericDate: &operator.NumericDateConfiguration{IsExcelDate: true}}),
		},
		{
			name:   "datetime_to_long_2_unix_millis",
			data:   getDatasetForConversion(types.CellValue{DataType: types.TimestampType, TimestampValue: time.Date(2023, 3, 28, 8, 35, 44, 533, time.UTC)}),
			want:   int64(1679992544000),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.LongType, NumericDate: &operator.NumericDateConfiguration{IsUnixMillis: true}}),
		},
		{
			name:   "datetime_to_double_unix_seconds",
			data:   getDatasetForConversion(types.CellValue{DataType: types.TimestampType, TimestampValue: time.Date(2023, 3, 28, 8, 35, 44, 533, time.UTC)}),
			want:   float64(1679992544),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.DoubleType, NumericDate: &operator.NumericDateConfiguration{IsUnixSeconds: true}}),
		},
		{
			name:   "datetime_to_bool",
			data:   getDatasetForConversion(types.CellValue{DataType: types.TimestampType, TimestampValue: time.Date(2023, 3, 28, 8, 35, 44, 533, time.UTC)}),
			want:   true,
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.BoolType}),
		},
		{
			name:   "datetime_to_date",
			data:   getDatasetForConversion(types.CellValue{DataType: types.TimestampType, TimestampValue: time.Date(2023, 3, 28, 8, 35, 44, 533, time.UTC)}),
			want:   time.Date(2023, 3, 28, 0, 0, 0, 0, time.UTC),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.DateType}),
		},
		{
			name:   "datetime_to_time",
			data:   getDatasetForConversion(types.CellValue{DataType: types.TimestampType, TimestampValue: time.Date(2023, 3, 28, 8, 35, 44, 533, time.UTC)}),
			want:   time.Date(0, 0, 0, 8, 35, 44, 533, time.UTC),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.TimeOfDayType}),
		},
		{
			name:   "datetime_to_time_CET",
			data:   getDatasetForConversion(types.CellValue{DataType: types.TimestampType, TimestampValue: time.Date(2023, 3, 28, 8, 35, 44, 533, time.UTC)}),
			want:   time.Date(0, 0, 0, 10, 35, 44, 533, time.UTC),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.TimeOfDayType, DateTimeDate: &operator.DateTimeDateConfiguration{Timezone: "CET"}}),
		},
		{
			name:   "datetime_to_time_CET_2",
			data:   getDatasetForConversion(types.CellValue{DataType: types.TimestampType, TimestampValue: time.Date(2023, 3, 5, 8, 35, 44, 533, time.UTC)}),
			want:   time.Date(0, 0, 0, 9, 35, 44, 533, time.UTC),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.TimeOfDayType, DateTimeDate: &operator.DateTimeDateConfiguration{Timezone: "CET"}}),
		},
	}
}

func getDateConversionTests() []conversionTest {
	return []conversionTest{
		{
			name:   "date_to_string_0",
			data:   getDatasetForConversion(types.CellValue{DataType: types.DateType, TimestampValue: time.Date(2021, 3, 18, 16, 45, 9, 0, time.UTC)}),
			want:   "2021-03-18",
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.StringType, StringDate: &operator.StringDateConfiguration{DateFormat: "yyyy-MM-dd"}}),
		},
		{
			name:   "date_to_string_1",
			data:   getDatasetForConversion(types.CellValue{DataType: types.DateType, TimestampValue: time.Date(2021, 3, 18, 16, 45, 9, 0, time.UTC)}),
			want:   "2021-03-18",
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.StringType, StringDate: &operator.StringDateConfiguration{DateFormat: "yyyy-MM-dd"}}),
		},
		{
			name:   "date_to_string_2",
			data:   getDatasetForConversion(types.CellValue{DataType: types.DateType, TimestampValue: time.Date(2021, 3, 18, 16, 45, 9, 0, time.UTC)}),
			want:   "2021-03-18",
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.StringType, StringDate: &operator.StringDateConfiguration{DateFormat: "yyyy-MM-dd"}}),
		},
		{
			name:   "date_to_string_3",
			data:   getDatasetForConversion(types.CellValue{DataType: types.DateType, TimestampValue: time.Date(2021, 3, 18, 16, 45, 9, 0, time.UTC)}),
			want:   "18.03.2021",
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.StringType, StringDate: &operator.StringDateConfiguration{DateFormat: "dd.MM.yyyy"}}),
		},
		{
			name:   "date_to_string_4",
			data:   getDatasetForConversion(types.CellValue{DataType: types.DateType, TimestampValue: time.Date(2021, 3, 18, 16, 45, 9, 0, time.UTC)}),
			want:   "18.03.2021 16:45:09",
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.StringType, StringDate: &operator.StringDateConfiguration{DateFormat: "dd.MM.yyyy hh:mm:ss"}}),
		},
		{
			name:   "date_to_string_5",
			data:   getDatasetForConversion(types.CellValue{DataType: types.DateType, TimestampValue: time.Date(2021, 3, 18, 16, 45, 9, 0, time.UTC)}),
			want:   "18.03.2021 16:45:09",
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.StringType, StringDate: &operator.StringDateConfiguration{DateFormat: "dd.MM.yyyy HH:mm:ss"}}),
		},
		{
			name:   "date_to_int_excel_date",
			data:   getDatasetForConversion(types.CellValue{DataType: types.DateType, TimestampValue: time.Date(2015, 1, 1, 16, 45, 9, 0, time.UTC)}),
			want:   int32(42005),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.IntType, NumericDate: &operator.NumericDateConfiguration{IsExcelDate: true}}),
		},
		{
			name:   "date_to_long_excel_date",
			data:   getDatasetForConversion(types.CellValue{DataType: types.DateType, TimestampValue: time.Date(2015, 1, 2, 16, 45, 9, 0, time.UTC)}),
			want:   int64(42006),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.LongType, NumericDate: &operator.NumericDateConfiguration{IsExcelDate: true}}),
		},
		{
			name:   "date_to_long_2_unix_millis",
			data:   getDatasetForConversion(types.CellValue{DataType: types.DateType, TimestampValue: time.Date(2023, 3, 28, 8, 35, 44, 533, time.UTC)}),
			want:   int64(1679992544000),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.LongType, NumericDate: &operator.NumericDateConfiguration{IsUnixMillis: true}}),
		},
		{
			name:   "date_to_double_unix_seconds",
			data:   getDatasetForConversion(types.CellValue{DataType: types.DateType, TimestampValue: time.Date(2023, 3, 28, 8, 35, 44, 533, time.UTC)}),
			want:   float64(1679992544),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.DoubleType, NumericDate: &operator.NumericDateConfiguration{IsUnixSeconds: true}}),
		},
		{
			name:   "date_to_bool",
			data:   getDatasetForConversion(types.CellValue{DataType: types.DateType, TimestampValue: time.Date(2023, 3, 28, 8, 35, 44, 533, time.UTC)}),
			want:   true,
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.BoolType}),
		},
		{
			name:   "date_to_datetime",
			data:   getDatasetForConversion(types.CellValue{DataType: types.DateType, TimestampValue: time.Date(2023, 3, 28, 8, 35, 44, 533, time.UTC)}),
			want:   time.Date(2023, 3, 28, 15, 44, 9, 0, time.UTC),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.TimestampType, DateTimeDate: &operator.DateTimeDateConfiguration{Timezone: "UTC", SelectedTime: "15:44:09"}}),
		},
	}
}

func getDoubleConversionTests() []conversionTest {
	return []conversionTest{
		{
			name:   "double_to_string_1",
			data:   getDatasetForConversion(types.CellValue{DataType: types.DoubleType, DoubleValue: 1233.456}),
			want:   "1.233,46",
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.StringType, StringNumeric: &operator.StringNumericConfiguration{DecimalSymbol: ",", NumberOfDecimals: 2, ThousandSeperator: "."}}),
		},
		{
			name:   "double_to_string_2",
			data:   getDatasetForConversion(types.CellValue{DataType: types.DoubleType, DoubleValue: 1233.456}),
			want:   "1 233.456",
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.StringType, StringNumeric: &operator.StringNumericConfiguration{DecimalSymbol: ".", NumberOfDecimals: 3, ThousandSeperator: " "}}),
		},
		{
			name:   "double_to_string_3",
			data:   getDatasetForConversion(types.CellValue{DataType: types.DoubleType, DoubleValue: 1123.456}),
			want:   "1,123.46",
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.StringType}),
		},
		{
			name:   "double_to_datetime_excel_date",
			data:   getDatasetForConversion(types.CellValue{DataType: types.DoubleType, DoubleValue: 42005.0}),
			want:   time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.TimestampType, NumericDate: &operator.NumericDateConfiguration{IsExcelDate: true}}),
		},
		{
			name:   "double_to_datetime_unix_millis",
			data:   getDatasetForConversion(types.CellValue{DataType: types.DoubleType, DoubleValue: 1679992544000}),
			want:   time.Date(2023, 3, 28, 8, 35, 44, 0, time.UTC),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.TimestampType, NumericDate: &operator.NumericDateConfiguration{IsUnixMillis: true}}),
		},
		{
			name:   "double_to_datetime_unix_seconds",
			data:   getDatasetForConversion(types.CellValue{DataType: types.DoubleType, DoubleValue: 1679992544}),
			want:   time.Date(2023, 3, 28, 8, 35, 44, 0, time.UTC),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.TimestampType, NumericDate: &operator.NumericDateConfiguration{IsUnixSeconds: true}}),
		},
		{
			name:   "double_to_date_excel_date",
			data:   getDatasetForConversion(types.CellValue{DataType: types.DoubleType, DoubleValue: 42005.0}),
			want:   time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.DateType, NumericDate: &operator.NumericDateConfiguration{IsExcelDate: true}}),
		},
		{
			name:   "double_to_date_unix_millis",
			data:   getDatasetForConversion(types.CellValue{DataType: types.DoubleType, DoubleValue: 1679992544000}),
			want:   time.Date(2023, 3, 28, 0, 0, 0, 0, time.UTC),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.DateType, NumericDate: &operator.NumericDateConfiguration{IsUnixMillis: true}}),
		},
		{
			name:   "double_to_date_unix_seconds",
			data:   getDatasetForConversion(types.CellValue{DataType: types.DoubleType, DoubleValue: 1679992544}),
			want:   time.Date(2023, 3, 28, 0, 0, 0, 0, time.UTC),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.DateType, NumericDate: &operator.NumericDateConfiguration{IsUnixSeconds: true}}),
		},
	}
}

func getLongConversionTests() []conversionTest {
	return []conversionTest{
		{
			name:   "long_to_string_1",
			data:   getDatasetForConversion(types.CellValue{DataType: types.LongType, LongValue: 1233}),
			want:   "1.233",
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.StringType, StringNumeric: &operator.StringNumericConfiguration{ThousandSeperator: "."}}),
		},
		{
			name:   "long_to_string_2",
			data:   getDatasetForConversion(types.CellValue{DataType: types.LongType, LongValue: 1233}),
			want:   "1 233",
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.StringType, StringNumeric: &operator.StringNumericConfiguration{ThousandSeperator: " "}}),
		},
		{
			name:   "long_to_string_3",
			data:   getDatasetForConversion(types.CellValue{DataType: types.LongType, LongValue: 1123}),
			want:   "1123",
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.StringType}),
		},
		{
			name:   "long_to_datetime_excel_date",
			data:   getDatasetForConversion(types.CellValue{DataType: types.LongType, LongValue: 42005.0}),
			want:   time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.TimestampType, NumericDate: &operator.NumericDateConfiguration{IsExcelDate: true}}),
		},
		{
			name:   "long_to_datetime_unix_millis",
			data:   getDatasetForConversion(types.CellValue{DataType: types.LongType, LongValue: 1679992544000}),
			want:   time.Date(2023, 3, 28, 8, 35, 44, 0, time.UTC),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.TimestampType, NumericDate: &operator.NumericDateConfiguration{IsUnixMillis: true}}),
		},
		{
			name:   "long_to_datetime_unix_seconds",
			data:   getDatasetForConversion(types.CellValue{DataType: types.LongType, LongValue: 1679992544}),
			want:   time.Date(2023, 3, 28, 8, 35, 44, 0, time.UTC),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.TimestampType, NumericDate: &operator.NumericDateConfiguration{IsUnixSeconds: true}}),
		},
		{
			name:   "long_to_date_excel_date",
			data:   getDatasetForConversion(types.CellValue{DataType: types.LongType, LongValue: 42005.0}),
			want:   time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.DateType, NumericDate: &operator.NumericDateConfiguration{IsExcelDate: true}}),
		},
		{
			name:   "long_to_date_unix_millis",
			data:   getDatasetForConversion(types.CellValue{DataType: types.LongType, LongValue: 1679992544000}),
			want:   time.Date(2023, 3, 28, 0, 0, 0, 0, time.UTC),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.DateType, NumericDate: &operator.NumericDateConfiguration{IsUnixMillis: true}}),
		},
		{
			name:   "long_to_date_unix_seconds",
			data:   getDatasetForConversion(types.CellValue{DataType: types.LongType, LongValue: 1679992544}),
			want:   time.Date(2023, 3, 28, 0, 0, 0, 0, time.UTC),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.DateType, NumericDate: &operator.NumericDateConfiguration{IsUnixSeconds: true}}),
		},
	}
}

func getIntConversionTests() []conversionTest {
	return []conversionTest{
		{
			name:   "int_to_string_1",
			data:   getDatasetForConversion(types.CellValue{DataType: types.IntType, IntValue: 1233}),
			want:   "1.233",
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.StringType, StringNumeric: &operator.StringNumericConfiguration{ThousandSeperator: "."}}),
		},
		{
			name:   "int_to_string_2",
			data:   getDatasetForConversion(types.CellValue{DataType: types.IntType, IntValue: 1233}),
			want:   "1 233",
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.StringType, StringNumeric: &operator.StringNumericConfiguration{ThousandSeperator: " "}}),
		},
		{
			name:   "int_to_string_3",
			data:   getDatasetForConversion(types.CellValue{DataType: types.IntType, IntValue: 1123}),
			want:   "1123",
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.StringType}),
		},
		{
			name:   "int_to_datetime_excel_date",
			data:   getDatasetForConversion(types.CellValue{DataType: types.IntType, IntValue: 42005.0}),
			want:   time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.TimestampType, NumericDate: &operator.NumericDateConfiguration{IsExcelDate: true}}),
		},
		{
			name:   "int_to_datetime_unix_seconds",
			data:   getDatasetForConversion(types.CellValue{DataType: types.IntType, IntValue: 1679992544}),
			want:   time.Date(2023, 3, 28, 8, 35, 44, 0, time.UTC),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.TimestampType, NumericDate: &operator.NumericDateConfiguration{IsUnixSeconds: true}}),
		},
		{
			name:   "int_to_date_excel_date",
			data:   getDatasetForConversion(types.CellValue{DataType: types.IntType, IntValue: 42005.0}),
			want:   time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.DateType, NumericDate: &operator.NumericDateConfiguration{IsExcelDate: true}}),
		},
		{
			name:   "int_to_date_unix_seconds",
			data:   getDatasetForConversion(types.CellValue{DataType: types.IntType, IntValue: 1679992544}),
			want:   time.Date(2023, 3, 28, 0, 0, 0, 0, time.UTC),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.DateType, NumericDate: &operator.NumericDateConfiguration{IsUnixSeconds: true}}),
		},
	}
}

func getTimeConversionTests() []conversionTest {
	return []conversionTest{
		{
			name:   "time_to_string_1",
			data:   getDatasetForConversion(types.CellValue{DataType: types.TimeOfDayType, TimestampValue: time.Date(2021, 3, 18, 16, 45, 9, 0, time.UTC)}),
			want:   "16:45:09",
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.StringType, StringDate: &operator.StringDateConfiguration{DateFormat: "hh:mm:ss"}}),
		},
		{
			name:   "time_to_string_default",
			data:   getDatasetForConversion(types.CellValue{DataType: types.TimeOfDayType, TimestampValue: time.Date(2021, 3, 18, 16, 45, 9, 0, time.UTC)}),
			want:   "16:45:09",
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.StringType}),
		},
		{
			name:   "time_to_datetime_default",
			data:   getDatasetForConversion(types.CellValue{DataType: types.TimeOfDayType, TimestampValue: time.Date(2021, 3, 18, 16, 45, 9, 0, time.UTC)}),
			want:   time.Date(0, 0, 0, 16, 45, 9, 0, time.UTC),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.TimestampType}),
		},
	}
}

func getStringConversionTests() []conversionTest {
	return []conversionTest{
		{
			name:   "string_to_long_1",
			data:   getDatasetForConversion(types.CellValue{DataType: types.StringType, StringValue: "1233"}),
			want:   int64(1233),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.LongType}),
		},
		{
			name:   "string_to_long_2",
			data:   getDatasetForConversion(types.CellValue{DataType: types.StringType, StringValue: "1,233"}),
			want:   int64(1233),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.LongType, StringNumeric: &operator.StringNumericConfiguration{ThousandSeperator: ","}}),
		},
		{
			name:   "string_to_long_3",
			data:   getDatasetForConversion(types.CellValue{DataType: types.StringType, StringValue: "1 233"}),
			want:   int64(1233),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.LongType, StringNumeric: &operator.StringNumericConfiguration{ThousandSeperator: " "}}),
		},
		{
			name:   "string_to_long_4",
			data:   getDatasetForConversion(types.CellValue{DataType: types.StringType, StringValue: "1 221 233"}),
			want:   int64(1221233),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.LongType, StringNumeric: &operator.StringNumericConfiguration{ThousandSeperator: " "}}),
		},
		{
			name:   "string_to_int_1",
			data:   getDatasetForConversion(types.CellValue{DataType: types.StringType, StringValue: "1233"}),
			want:   int32(1233),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.IntType}),
		},
		{
			name:   "string_to_int_2",
			data:   getDatasetForConversion(types.CellValue{DataType: types.StringType, StringValue: "1,233"}),
			want:   int32(1233),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.IntType, StringNumeric: &operator.StringNumericConfiguration{ThousandSeperator: ","}}),
		},
		{
			name:   "string_to_int_3",
			data:   getDatasetForConversion(types.CellValue{DataType: types.StringType, StringValue: "1 233"}),
			want:   int32(1233),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.IntType, StringNumeric: &operator.StringNumericConfiguration{ThousandSeperator: " "}}),
		},
		{
			name:   "string_to_double_1",
			data:   getDatasetForConversion(types.CellValue{DataType: types.StringType, StringValue: "1233"}),
			want:   float64(1233),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.DoubleType}),
		},
		{
			name:   "string_to_double_2",
			data:   getDatasetForConversion(types.CellValue{DataType: types.StringType, StringValue: "1,233"}),
			want:   float64(1233),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.DoubleType, StringNumeric: &operator.StringNumericConfiguration{ThousandSeperator: ","}}),
		},
		{
			name:   "string_to_double_3",
			data:   getDatasetForConversion(types.CellValue{DataType: types.StringType, StringValue: "1 233"}),
			want:   float64(1233),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.DoubleType, StringNumeric: &operator.StringNumericConfiguration{ThousandSeperator: " "}}),
		},
		{
			name:   "string_to_bool_1",
			data:   getDatasetForConversion(types.CellValue{DataType: types.StringType, StringValue: "TrUe"}),
			want:   true,
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.BoolType}),
		},
		{
			name:   "string_to_bool_2",
			data:   getDatasetForConversion(types.CellValue{DataType: types.StringType, StringValue: "False"}),
			want:   false,
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.BoolType}),
		},
		{
			name:   "string_to_datetime_1",
			data:   getDatasetForConversion(types.CellValue{DataType: types.StringType, StringValue: "2021-03-18 16:45:09"}),
			want:   time.Date(2021, 3, 18, 16, 45, 9, 0, time.UTC),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.TimestampType, StringDate: &operator.StringDateConfiguration{DateFormat: "yyyy-MM-dd hh:mm:ss"}}),
		},
		{
			name:   "string_to_datetime_2",
			data:   getDatasetForConversion(types.CellValue{DataType: types.StringType, StringValue: "2021-03-18"}),
			want:   time.Date(2021, 3, 18, 0, 0, 0, 0, time.UTC),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.TimestampType, StringDate: &operator.StringDateConfiguration{DateFormat: "yyyy-MM-dd"}}),
		},
		{
			name:   "string_to_datetime_3",
			data:   getDatasetForConversion(types.CellValue{DataType: types.StringType, StringValue: "16:45:09"}),
			want:   time.Date(0, 1, 1, 16, 45, 9, 0, time.UTC),
			config: getConfigForConversionColumn(operator.ConversionConfiguration{TargetType: types.TimestampType, StringDate: &operator.StringDateConfiguration{DateFormat: "hh:mm:ss"}}),
		},
	}
}

func TestChangeColumnTypeCombinations(t *testing.T) {
	tests := []conversionTest{}
	tests = append(tests, getDateTimeConversionTests()...)
	tests = append(tests, getDateConversionTests()...)
	tests = append(tests, getDoubleConversionTests()...)
	tests = append(tests, getLongConversionTests()...)
	tests = append(tests, getIntConversionTests()...)
	tests = append(tests, getTimeConversionTests()...)
	tests = append(tests, getStringConversionTests()...)

	changeColumnTypeOp := operator.ChangeColumnTypeOperator{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)
			converted, err := changeColumnTypeOp.Transform(&tt.data, tt.config, nil)
			assert.Nil(err, "Column type conversion failed")
			assert.Equal(tt.want, converted.Rows[0].Columns[0].CellValue.Value(), "Column type conversion failed")
		})
	}
}
