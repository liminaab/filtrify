package operatorv2

import (
	"encoding/json"
	"testing"

	"github.com/liminaab/filtrify/conversion"
	"github.com/liminaab/filtrify/operator"
	"github.com/liminaab/filtrify/test"
	"github.com/liminaab/filtrify/types"
)

// These tests pin the v2 JSON operator to the v1 one: every case runs both
// implementations over the same raw data and the same configuration JSON, then
// compares the results column by column (assertSameOutput, from lookup_test.go)
// and reads the JSON column back to check the object itself.

type jsonCase struct {
	name   string
	data   [][]string
	config *JSONConfiguration
}

// run converts the raw data for both generations, runs both operators and
// returns their outputs along with the errors they produced.
func (c jsonCase) run(t *testing.T) (*types.DataSet, *types.DataSetv2, error, error) {
	t.Helper()

	cfgJSON, err := json.Marshal(c.config)
	if err != nil {
		t.Fatalf("could not marshal the configuration: %v", err)
	}

	v1In, err := conversion.ConvertToTypedData(c.data, true, true, nil, true)
	if err != nil {
		t.Fatalf("v1 conversion failed: %v", err)
	}
	v2In, err := conversion.ConvertToTypedDatav2(c.data, true, true, nil, true)
	if err != nil {
		t.Fatalf("v2 conversion failed: %v", err)
	}

	outV1, errV1 := (&operator.JSONOperator{}).Transform(v1In, string(cfgJSON), nil)
	outV2, errV2 := (&JSONOperator{}).Transform(v2In, string(cfgJSON), nil)
	return outV1, outV2, errV1, errV2
}

func runJSONParityCase(t *testing.T, c jsonCase) (*types.DataSet, *types.DataSetv2) {
	t.Helper()
	outV1, outV2, errV1, errV2 := c.run(t)
	if errV1 != nil {
		t.Fatalf("v1 json failed: %v", errV1)
	}
	if errV2 != nil {
		t.Fatalf("v2 json failed: %v", errV2)
	}
	assertSameOutput(t, outV1, outV2)
	return outV1, outV2
}

// jsonObjects reads the target column of a v2 result back into one map per row.
func jsonObjects(t *testing.T, ds *types.DataSetv2, target string) []map[string]string {
	t.Helper()
	col := ds.GetColumn(target)
	if col == nil {
		t.Fatalf("the result has no column %q", target)
	}
	flat := newFlatColumn(col)
	out := make([]map[string]string, ds.NumRows())
	for row := int64(0); row < ds.NumRows(); row++ {
		arr, idx, found := flat.at(row)
		if !found {
			t.Fatalf("column %q has no row %d", target, row)
		}
		if arr.IsNull(idx) {
			t.Fatalf("column %q row %d is null", target, row)
		}
		obj := make(map[string]string)
		if err := json.Unmarshal([]byte(jsonCellText(arr, idx)), &obj); err != nil {
			t.Fatalf("column %q row %d is not a JSON object: %v", target, row, err)
		}
		out[row] = obj
	}
	return out
}

func columnNames(ds *types.DataSetv2) []string {
	names := make([]string, ds.NumCols())
	for i := range ds.NumCols() {
		names[i] = ds.Column(int(i)).Name()
	}
	return names
}

func TestJSONv2Basic(t *testing.T) {
	_, outV2 := runJSONParityCase(t, jsonCase{
		name: "two string columns",
		data: test.UAT1TestDataFormatted,
		config: &JSONConfiguration{
			Fields:          []string{"Instrument Type", "Instrument name"},
			TargetFieldName: "jsonified",
		},
	})

	// the jsonified columns are gone and the target sits at the end
	want := []string{"Quantity", "Market Value (Base)", "Exposure %", "Maturity Date",
		"EU Sanction listed", "Active From", "jsonified"}
	got := columnNames(outV2)
	if len(got) != len(want) {
		t.Fatalf("columns = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("columns = %v, want %v", got, want)
		}
	}

	objects := jsonObjects(t, outV2, "jsonified")
	for row, obj := range objects {
		if len(obj) != 2 {
			t.Errorf("row %d object = %v, want 2 keys", row, obj)
		}
		// row+1 skips the header line of the raw data; columns 0 and 1 are the
		// instrument name and type
		if obj["Instrument name"] != test.UAT1TestDataFormatted[row+1][0] {
			t.Errorf("row %d: Instrument name = %q, want %q",
				row, obj["Instrument name"], test.UAT1TestDataFormatted[row+1][0])
		}
		if obj["Instrument Type"] != test.UAT1TestDataFormatted[row+1][1] {
			t.Errorf("row %d: Instrument Type = %q, want %q",
				row, obj["Instrument Type"], test.UAT1TestDataFormatted[row+1][1])
		}
	}
}

// The target column already exists: v1 drops it wherever it sat and appends the
// JSON column at the end, so the column survives by name but moves and changes
// type.
func TestJSONv2ReplacesExistingColumn(t *testing.T) {
	_, outV2 := runJSONParityCase(t, jsonCase{
		name: "target is an existing column",
		data: test.UAT1TestDataFormatted,
		config: &JSONConfiguration{
			Fields:          []string{"Instrument Type", "Instrument name"},
			TargetFieldName: "Quantity",
		},
	})

	last := outV2.Column(int(outV2.NumCols()) - 1)
	if last.Name() != "Quantity" {
		t.Fatalf("last column is %q, want %q", last.Name(), "Quantity")
	}
	if conversion.FiltrifyType(last.DataType()) != types.StringType {
		t.Errorf("the json column is %v, want a string", last.DataType())
	}
	if n := len(jsonObjects(t, outV2, "Quantity")); n != int(outV2.NumRows()) {
		t.Errorf("read back %d objects, the table has %d rows", n, outV2.NumRows())
	}
}

// The target column is also listed as a field. v1 skips it before it ever looks
// at the field map, so it is dropped rather than jsonified.
func TestJSONv2TargetIsAlsoAField(t *testing.T) {
	_, outV2 := runJSONParityCase(t, jsonCase{
		name: "target listed among the fields",
		data: test.UAT1TestDataFormatted,
		config: &JSONConfiguration{
			Fields:          []string{"Quantity", "Instrument name"},
			TargetFieldName: "Quantity",
		},
	})

	for _, obj := range jsonObjects(t, outV2, "Quantity") {
		if _, exists := obj["Quantity"]; exists {
			t.Fatalf("the target column was jsonified into itself: %v", obj)
		}
		if len(obj) != 1 {
			t.Fatalf("object = %v, want only the Instrument name key", obj)
		}
	}
}

// Every non-string type goes through jsonCellText: int, double, bool, timestamp
// and the empty cells that become nulls. The parity check is the point here -
// v1 renders these with CellValue.ToString.
func TestJSONv2AllColumnTypes(t *testing.T) {
	_, outV2 := runJSONParityCase(t, jsonCase{
		name: "every column of the dataset",
		data: test.UAT1TestDataFormatted,
		config: &JSONConfiguration{
			Fields: []string{"Instrument name", "Instrument Type", "Quantity",
				"Market Value (Base)", "Exposure %", "Maturity Date",
				"EU Sanction listed", "Active From"},
			TargetFieldName: "jsonified",
		},
	})

	if outV2.NumCols() != 1 {
		t.Fatalf("columns = %v, want the json column alone", columnNames(outV2))
	}
	objects := jsonObjects(t, outV2, "jsonified")
	// the first row's maturity date is empty, so it renders as ""
	if objects[0]["Maturity Date"] != "" {
		t.Errorf("row 0 Maturity Date = %q, want an empty string", objects[0]["Maturity Date"])
	}
	if objects[0]["EU Sanction listed"] != "true" {
		t.Errorf("row 0 EU Sanction listed = %q, want %q", objects[0]["EU Sanction listed"], "true")
	}
}

// A date column and a time-of-day column, which arrow stores as Date32 and
// Time64 rather than as timestamps.
func TestJSONv2DateAndTimeColumns(t *testing.T) {
	runJSONParityCase(t, jsonCase{
		name: "date column",
		data: test.UAT1TestDataFormattedWithDate,
		config: &JSONConfiguration{
			Fields:          []string{"Active From", "Maturity Date"},
			TargetFieldName: "jsonified",
		},
	})
	runJSONParityCase(t, jsonCase{
		name: "time of day column",
		data: test.UAT1TestDataFormattedWithTime,
		config: &JSONConfiguration{
			Fields:          []string{"Active From"},
			TargetFieldName: "jsonified",
		},
	})
}

// A field naming a column the dataset does not have is absent from the object,
// as in v1, and the JSON column is still built when none of the fields match.
func TestJSONv2UnknownFields(t *testing.T) {
	_, outV2 := runJSONParityCase(t, jsonCase{
		name: "one known and one unknown field",
		data: test.UAT1TestDataFormatted,
		config: &JSONConfiguration{
			Fields:          []string{"Instrument name", "no such column"},
			TargetFieldName: "jsonified",
		},
	})
	for row, obj := range jsonObjects(t, outV2, "jsonified") {
		if len(obj) != 1 {
			t.Fatalf("row %d object = %v, want only the Instrument name key", row, obj)
		}
	}

	_, outV2 = runJSONParityCase(t, jsonCase{
		name: "nothing but unknown fields",
		data: test.UAT1TestDataFormatted,
		config: &JSONConfiguration{
			Fields:          []string{"no such column"},
			TargetFieldName: "jsonified",
		},
	})
	for row, obj := range jsonObjects(t, outV2, "jsonified") {
		if len(obj) != 0 {
			t.Fatalf("row %d object = %v, want an empty object", row, obj)
		}
	}
}

// Chunked.Release() sets chunks=nil when the refcount reaches 0, so a non-empty
// Chunks() after the owning tables are released proves a refcount was never
// given back.
func TestJSONv2ColumnRefcount(t *testing.T) {
	rows := [][]string{{"a", "b", "c"}, {"1", "2", "3"}, {"4", "5", "6"}}

	ds, err := conversion.ConvertToTypedDatav2(rows, true, true, nil, true)
	if err != nil {
		t.Fatalf("conversion: %v", err)
	}

	out, err := (&JSONOperator{}).Transform(ds, `{"fields":["b"],"targetFieldName":"j"}`, nil)
	if err != nil {
		t.Fatalf("transform: %v", err)
	}

	// column "a" is passed through, "b" was folded into the json column, and
	// "j" is the json column itself
	passthrough := out.GetColumn("a").Data()
	jsonified := ds.GetColumn("b").Data()
	built := out.GetColumn("j").Data()

	out.Release()
	ds.Release()

	if n := len(passthrough.Chunks()); n != 0 {
		t.Errorf("passthrough column leaked: %d chunks still alive", n)
	}
	if n := len(jsonified.Chunks()); n != 0 {
		t.Errorf("jsonified column leaked: %d chunks still alive", n)
	}
	if n := len(built.Chunks()); n != 0 {
		t.Errorf("json column leaked: %d chunks still alive", n)
	}
}

// The configuration is validated exactly as v1 validates it.
func TestJSONv2ConfigurationParity(t *testing.T) {
	cases := []struct {
		name   string
		config string
	}{
		{"empty", ""},
		{"not json", "{"},
		{"no fields", `{"targetFieldName":"jsonified"}`},
		{"empty field list", `{"fields":[],"targetFieldName":"jsonified"}`},
		{"no target", `{"fields":["Quantity"]}`},
		{"empty field name", `{"fields":[""],"targetFieldName":"jsonified"}`},
		{"valid", `{"fields":["Quantity"],"targetFieldName":"jsonified"}`},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			okV1, errV1 := (&operator.JSONOperator{}).ValidateConfiguration(c.config)
			okV2, errV2 := (&JSONOperator{}).ValidateConfiguration(c.config)
			if okV1 != okV2 {
				t.Errorf("valid: v1 = %v, v2 = %v", okV1, okV2)
			}
			switch {
			case errV1 == nil && errV2 == nil:
			case errV1 == nil || errV2 == nil:
				t.Errorf("error: v1 = %v, v2 = %v", errV1, errV2)
			case errV1.Error() != errV2.Error():
				t.Errorf("error: v1 = %q, v2 = %q", errV1, errV2)
			}
		})
	}
}
