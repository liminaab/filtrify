package operatorv2

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/liminaab/filtrify/types"
)

type JSONOperator struct {
}

type JSONConfiguration struct {
	Fields          []string `json:"fields"`
	TargetFieldName string   `json:"targetFieldName"`
}

// jsonField is one of the columns being folded into the JSON column: its name,
// which becomes the JSON key, and random access to its values.
type jsonField struct {
	name string
	flat *flatColumn
}

// Transform replaces the configured fields with a single string column holding
// one JSON object per row, keyed by column name. It reproduces v1
// (operator/json.go) exactly:
//
//   - the configured fields leave the output and become keys of the object,
//   - a column already called TargetFieldName is dropped wherever it sat - so it
//     is never one of the jsonified keys either, even when the configuration
//     lists it - and the JSON column is appended at the end,
//   - every remaining column keeps its place and its data, and
//   - each value is rendered the way CellValue.ToString would render the
//     equivalent v1 cell, with a null becoming an empty string.
//
// A field naming a column the dataset does not have is simply absent from the
// object, as in v1; so the JSON column is still built - as "{}" in every row -
// when none of the fields match anything.
func (t *JSONOperator) Transform(dataset *types.DataSetv2, config string, _ map[string]*types.DataSetv2) (*types.DataSetv2, error) {

	typedConfig, confError := t.buildConfiguration(config)
	if confError != nil {
		return nil, confError
	}

	if dataset == nil || dataset.Table == nil {
		return nil, errors.New("cannot jsonify a nil dataset")
	}

	fieldMap := make(map[string]bool)
	for _, field := range typedConfig.Fields {
		fieldMap[field] = true
	}

	newColumns := make([]arrow.Column, 0, int(dataset.NumCols())+1)
	// every entry of newColumns is a reference we own - NewTable takes its own,
	// so we hand ours back once we are done
	defer func() {
		for i := range newColumns {
			newColumns[i].Release()
		}
	}()

	jsonFields := make([]jsonField, 0, len(typedConfig.Fields))
	for i := range dataset.NumCols() {
		currentCol := dataset.Column(int(i))
		if currentCol.Name() == typedConfig.TargetFieldName {
			// the target column is rebuilt from scratch below, so this one goes
			// away whether or not the configuration also lists it as a field
			continue
		}
		if fieldMap[currentCol.Name()] {
			// this column is going to be removed and converted into JSON
			jsonFields = append(jsonFields, jsonField{name: currentCol.Name(), flat: newFlatColumn(currentCol)})
			continue
		}
		currentCol.Retain()
		newColumns = append(newColumns, *currentCol)
	}

	jsonColumn, err := buildJSONColumn(typedConfig.TargetFieldName, jsonFields, dataset.NumRows())
	if err != nil {
		return nil, err
	}
	newColumns = append(newColumns, *jsonColumn)

	fields := make([]arrow.Field, len(newColumns))
	for i := range newColumns {
		fields[i] = newColumns[i].Field()
	}
	oldMetadata := dataset.Schema().Metadata()
	schema := arrow.NewSchema(fields, &oldMetadata)
	return &types.DataSetv2{Table: array.NewTable(schema, newColumns, dataset.NumRows())}, nil
}

// buildJSONColumn renders one JSON object per row out of fields. The returned
// column is an owned reference - the caller releases it. json.Marshal of a
// map[string]string orders the keys alphabetically, which is what v1's output
// looks like too.
func buildJSONColumn(name string, fields []jsonField, numRows int64) (*arrow.Column, error) {
	builder := array.NewStringBuilder(memory.DefaultAllocator)
	defer builder.Release()
	builder.Reserve(int(numRows))

	values := make(map[string]string, len(fields))
	for row := int64(0); row < numRows; row++ {
		for k := range values {
			delete(values, k)
		}
		for _, f := range fields {
			arr, idx, found := f.flat.at(row)
			if !found {
				return nil, fmt.Errorf("column %q has no row %d", f.name, row)
			}
			values[f.name] = jsonCellText(arr, idx)
		}
		encoded, err := json.Marshal(values)
		if err != nil {
			return nil, err
		}
		builder.Append(string(encoded))
	}

	arr := builder.NewArray()
	defer arr.Release()

	field := arrow.Field{Name: name, Type: arrow.BinaryTypes.String, Nullable: true}
	chunked := arrow.NewChunked(field.Type, []arrow.Array{arr}) // retains arr
	defer chunked.Release()
	return arrow.NewColumn(field, chunked), nil // retains chunked
}

// jsonCellText renders slot i of arr the way CellValue.ToString renders the
// equivalent v1 cell, so the objects this operator writes hold the same text v1
// wrote. A null - which is v1's NilType cell - renders as an empty string.
func jsonCellText(arr arrow.Array, i int) string {
	if arr.IsNull(i) {
		return ""
	}
	switch a := arr.(type) {
	case *array.Null:
		return ""
	case *array.Int32:
		return strconv.FormatInt(int64(a.Value(i)), 10)
	case *array.Int64:
		return strconv.FormatInt(a.Value(i), 10)
	case *array.Float64:
		return strconv.FormatFloat(a.Value(i), 'f', -1, 64)
	case *array.Boolean:
		if a.Value(i) {
			return "true"
		}
		return "false"
	case *array.String:
		return a.Value(i)
	case *array.LargeString:
		// object columns are stored as JSON text, which is what v1's ToString
		// marshals its object cell into
		return a.Value(i)
	case *array.Timestamp:
		unit := a.DataType().(*arrow.TimestampType).Unit
		return a.Value(i).ToTime(unit).Format(time.RFC3339)
	case *array.Date32:
		return a.Value(i).ToTime().Format("2006-01-02")
	case *array.Time64:
		unit := a.DataType().(*arrow.Time64Type).Unit
		return a.Value(i).ToTime(unit).Format("15:04:05")
	default:
		return arr.ValueStr(i)
	}
}

func (t *JSONOperator) buildConfiguration(config string) (*JSONConfiguration, error) {
	if len(config) < 1 {
		return nil, errors.New("invalid configuration")
	}
	// config is a json declaration of our field configuration
	typedConfig := JSONConfiguration{}
	err := json.Unmarshal([]byte(config), &typedConfig)
	if err != nil {
		return nil, err
	}

	if len(typedConfig.Fields) < 1 {
		return nil, errors.New("missing json configuration")
	}

	if len(typedConfig.TargetFieldName) < 1 {
		return nil, errors.New("missing json configuration")
	}

	for _, ob := range typedConfig.Fields {
		if len(ob) < 1 {
			return nil, errors.New("missing column name in json configuration")
		}
	}

	return &typedConfig, nil
}

func (t *JSONOperator) ValidateConfiguration(config string) (bool, error) {
	typedConfig, err := t.buildConfiguration(config)
	return typedConfig != nil, err
}
