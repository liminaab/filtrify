package test

import (
	"fmt"
	"strconv"
	"time"

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

var UATLookupTestDataFormatted [][]string = [][]string{
	{"Instrument name", "Instrument ID", "Quantity", "ISIN", "Currency"},
	{"ERIC B SS Equity", "1", "175 000.00", "SE0000108656", "SEK"},
	{"AMZN US Equity", "2", "1 500.00", "US0231351067", "USD"},
	{"T 0 12/31/21", "3", "9 000 000.00", "US0231399991", "USD"},
	{"ESZ1", "4", "-10.00", "", "USD"},
	{"USD Cash", "", "5 000 000.00", "", "USD"},
	{"ERIC B LN Equity", "5", "175 000.00", "SE0000108656", "CHF"},
}

var UATLookupJoinTestDataFormatted [][]string = [][]string{
	{"Instrument ID", "Instrument name", "ISIN", "Currency", "Region"},
	{"1", "ERIC B SS Equity", "SE0000108656", "SEK", "Europe"},
	{"2", "AMZN US Equity", "US0231351067", "USD", "Americas"},
	{"3", "T 0 12/31/21", "US0231399991", "USD", "Americas"},
	{"4", "ESZ1", "", "USD", "Americas"},
	{"5", "ERIC B LN Equity", "SE0000108656", "CHF", "Europe"},
}

var UATMappedValueTestDataFormatted [][]string = [][]string{
	{"Instrument name", "Broker ID", "Quantity"},
	{"ERIC B SS Equity", "1", "175 000.00"},
	{"AMZN US Equity", "2", "1 500.00"},
	{"T 0 12/31/21", "1", "9 000 000.00"},
	{"ESZ1", "1", "-10.00"},
	{"USD Cash", "", "5 000 000.00"},
	{"ERIC B LN Equity", "3", "175 000.00"},
}

var UATMappedValueMapTestDataFormatted [][]string = [][]string{
	{"Key", "Value"},
	{"1", "Goldman Sachs Int."},
	{"2", "UBS"},
	{"3", "Credit Suisse"},
	{"4", "SEB"},
}

func CopyColumn(col *types.DataColumn) *types.DataColumn {

	cellVal := &types.CellValue{
		DataType: col.CellValue.DataType,
	}
	switch cellVal.DataType {
	case types.IntType:
		cellVal.IntValue = col.CellValue.IntValue
		break
	case types.LongType:
		cellVal.LongValue = col.CellValue.LongValue
		break
	case types.TimestampType:
		cellVal.TimestampValue = col.CellValue.TimestampValue
		break
	case types.StringType:
		cellVal.StringValue = col.CellValue.StringValue
		break
	case types.DoubleType:
		cellVal.DoubleValue = col.CellValue.DoubleValue
		break
	case types.BoolType:
		cellVal.BoolValue = col.CellValue.BoolValue
		break
	}

	newCol := &types.DataColumn{
		ColumnName: col.ColumnName,
		CellValue:  cellVal,
	}

	return newCol
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

func HasSameValues(cell1 *types.CellValue, cell2 *types.CellValue) bool {
	if cell1 == nil || cell2 == nil {
		return false
	}

	if cell1.DataType != cell2.DataType {
		return false
	}

	if cell1.DataType == types.NilType || cell2.DataType == types.NilType {
		return false
	}

	switch cell1.DataType {
	case types.IntType:
		return cell1.IntValue == cell2.IntValue
	case types.LongType:
		return cell1.LongValue == cell2.LongValue
	case types.TimestampType:
		return cell1.TimestampValue.Equal(cell2.TimestampValue)
	case types.StringType:
		return cell1.StringValue == cell2.StringValue
	case types.DoubleType:
		return cell1.DoubleValue == cell2.DoubleValue
	case types.BoolType:
		return cell1.BoolValue == cell2.BoolValue
	}

	return false
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
		if w, ok := val.(time.Time); ok {
			return cell.TimestampValue.Equal(w)
		}
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
