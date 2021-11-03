package test

import (
	"fmt"
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
	{"AMZN US Equity", "Equity", "1500.00", "6000000.00", "25%", "", "true", "2020-03-01 12:00:00", "USD"},
	{"T 0 12/31/21", "Bill", "9000000.00", "8750000.00", "30%", "2021-12-31", "false", "2020-11-22 12:00:00", "USD"},
	{"ESZ1", "Index Future", "-10.00", "-495000.00", "17%", "2021-12-16", "false", "2021-04-06 12:00:00", "USD"},
	{"USD Cash", "Cash Account", "5000000.00", "5000000.0", "20%", "", "", "2020-01-01 12:00:00", "USD"},
}

func PrintDataset(ds *types.DataSet) {
	if len(ds.Rows) < 1 {
		fmt.Println("=============== NO DATA ===============")
		return
	}

	// print headers here
	row0 := ds.Rows[0]
	for _, col := range row0.Columns {
		fmt.Print(col.ColumnName)
		fmt.Print("  |  ")
	}
	fmt.Println("")
	fmt.Println("----------------------------------------")
	for _, r := range ds.Rows {
		for _, c := range r.Columns {
			fmt.Print(CellDataToString(c.CellValue))
			fmt.Print("  |  ")
		}
		fmt.Println("")
		fmt.Println("----------------------------------------")
	}
}

func GetColumn(r *types.DataRow, col string) *types.DataColumn {
	for _, c := range r.Columns {
		if c.ColumnName == col {
			return c
		}
	}

	return nil
}

func IsEqual(cell *types.CellValue, val interface{}) bool {
	if cell == nil && val == nil {
		return true
	}

	if cell == nil {
		return false
	}

	switch cell.DataType {
	case types.IntType:
		if w, ok := val.(int32); ok {
			return w == cell.IntValue
		}
		return false
	case types.LongType:
		if w, ok := val.(int64); ok {
			return w == cell.LongValue
		}
		return false
	case types.TimestampType:
		// TODO implement
		return false
	case types.StringType:
		if w, ok := val.(string); ok {
			return w == cell.StringValue
		}
		return false
	case types.DoubleType:
		if w, ok := val.(float64); ok {
			return w == cell.DoubleValue
		}
		return false
	case types.BoolType:
		if w, ok := val.(bool); ok {
			return w == cell.BoolValue
		}
		return false
	case types.NilType:
		return val == nil

	}

	return false
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
