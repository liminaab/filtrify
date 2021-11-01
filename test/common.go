package test

import (
	"strconv"

	"limina.com/dyntransformer/types"
)

var uat1TestData [][]string = [][]string{
	{"Instrument name", "Instrument Type", "Quantity", "Market Value (Base)", "Exposure %", "Maturity Date", "EU Sanction listed", "Active From"},
	{"ERIC B SS Equity", "Equity", "175 000.00", "2 000 000.00", "8%", "", "true", "2020-01-01 12:00:00"},
	{"AMZN US Equity", "Equity", "1 500.00", "6 000 000.00", "25%", "", "false", "2020-03-01 12:00:00"},
	{"T 0 12/31/21", "Bill", "9 000 000.00", "8 750 000.00", "30%", "2021-12-31", "false", "2020-11-22 12:00:00"},
	{"ESZ1", "Index Future", "-10.00", "-495 000.00", "17%", "2021-12-16", "false", "2021-04-06 12:00:00"},
	{"USD Cash", "Cash Account", "5 000 000.00", "5 000 000.0", "20%", "", "", "2020-01-01 12:00:00"},
}

var UAT1TestDataFormatted [][]string = [][]string{
	{"Instrument name", "Instrument Type", "Quantity", "Market Value (Base)", "Exposure %", "Maturity Date", "EU Sanction listed", "Active From"},
	{"ERIC B SS Equity", "Equity", "175000.00", "2000000.00", "8%", "", "true", "2020-01-01 12:00:00"},
	{"AMZN US Equity", "Equity", "1500.00", "6000000.00", "25%", "", "false", "2020-03-01 12:00:00"},
	{"T 0 12/31/21", "Bill", "9000000.00", "8750000.00", "30%", "2021-12-31", "false", "2020-11-22 12:00:00"},
	{"ESZ1", "Index Future", "-10.00", "-495000.00", "17%", "2021-12-16", "false", "2021-04-06 12:00:00"},
	{"USD Cash", "Cash Account", "5000000.00", "5000000.0", "20%", "", "", "2020-01-01 12:00:00"},
}

var UATAggregateTestDataFormatted [][]string = [][]string{
	{"Instrument name", "Instrument Type", "Quantity", "Market Value (Base)", "Exposure %", "Maturity Date", "EU Sanction listed", "Active From", "Currency"},
	{"ERIC B SS Equity", "Equity", "175000.00", "2000000.00", "8%", "", "true", "2020-01-01 12:00:00", "SEK"},
	{"AMZN US Equity", "Equity", "1500.00", "6000000.00", "25%", "", "false", "2020-03-01 12:00:00", "USD"},
	{"T 0 12/31/21", "Bill", "9000000.00", "8750000.00", "30%", "2021-12-31", "false", "2020-11-22 12:00:00", "USD"},
	{"ESZ1", "Index Future", "-10.00", "-495000.00", "17%", "2021-12-16", "false", "2021-04-06 12:00:00", "USD"},
	{"USD Cash", "Cash Account", "5000000.00", "5000000.0", "20%", "", "", "2020-01-01 12:00:00", "USD"},
}

func GetColumn(r *types.DataRow, col string) *types.DataColumn {
	for _, c := range r.Columns {
		if c.ColumnName == col {
			return c
		}
	}

	return nil
}

func CellDataToString(cell *types.CellValue) string {
	if cell == nil {
		return ""
	}

	switch cell.DataType {
	case types.IntType:
		return strconv.FormatInt(int64(cell.IntValue), 10)
	case types.LongType:
		return strconv.FormatInt(cell.LongValue, 10)
	case types.TimestampType:
		return cell.TimestampValue.String()
	case types.StringType:
		return cell.StringValue
	case types.DoubleType:
		return strconv.FormatFloat(cell.DoubleValue, 'f', 6, 64)
	case types.BoolType:
		if cell.BoolValue {
			return "true"
		}
		return "false"
	case types.NilType:
		return ""

	}

	return ""
}
