package lmnqlbridge

import (
	"database/sql/driver"
	"encoding/json"
	"strings"

	"cloud.google.com/go/civil"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/liminaab/filtrify/conversion"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
	"github.com/liminaab/filtrify/types"
)

type LmnInMemTablev2 struct {
	table    *schema.Table
	dataset  *types.DataSetv2
	headers  []string
	exit     <-chan bool
	rowCount uint64
	colindex map[string]int
}

type LmnInMemDataSourcev2 struct {
	tables map[string]*LmnInMemTablev2
	exit   <-chan bool
}

// NewCsvSource reader assumes we are getting first row as headers
// - optionally may be gzipped
func NewLmnInMemDataSourcev2(exit <-chan bool) *LmnInMemDataSourcev2 {
	m := LmnInMemDataSourcev2{tables: make(map[string]*LmnInMemTablev2)}
	return &m
}

func (m *LmnInMemDataSourcev2) ReplaceTable(name string, dataset *types.DataSetv2) {
	delete(m.tables, name)
	m.AddTable(name, dataset)
}

func (m *LmnInMemDataSourcev2) mapToInternalType(t types.CellDataType) value.ValueType {
	switch t {
	case types.TimestampType:
		return value.TimeType
	case types.TimeOfDayType:
		return value.TimeOnlyType
	case types.DateType:
		return value.DateType
	case types.IntType:
		return value.IntType
	case types.LongType:
		return value.IntType
	case types.DoubleType:
		return value.NumberType
	case types.BoolType:
		return value.BoolType
	case types.StringType:
		return value.StringType
	case types.NilType:
		return value.NilType
	case types.ObjectType:
		return value.MapValueType
	}

	return value.NilType
}

func (m *LmnInMemDataSourcev2) AddTable(name string, dataset *types.DataSetv2) {
	inMemTable := &LmnInMemTablev2{
		dataset:  dataset,
		exit:     m.exit,
		rowCount: 0,
	}
	inMemTable.table = schema.NewTable(strings.ToLower(name))
	inMemTable.headers = make([]string, dataset.NumCols())
	colindex := make(map[string]int, dataset.NumCols())
	// let's calculate headers
	for i := range dataset.NumCols() {
		theCol := dataset.Column(int(i))
		inMemTable.headers[i] = theCol.Name()
		colindex[inMemTable.headers[i]] = int(i)

		internalType := m.mapToInternalType(conversion.FiltrifyType(theCol.DataType()))
		inMemTable.table.AddField(schema.NewFieldBase(theCol.Name(), internalType, 64, theCol.Name()))
	}

	inMemTable.colindex = colindex
	inMemTable.dataset = dataset
	m.tables[name] = inMemTable
}

func (m *LmnInMemDataSourcev2) Init()                      {}
func (m *LmnInMemDataSourcev2) Setup(*schema.Schema) error { return nil }
func (m *LmnInMemDataSourcev2) Tables() []string {
	tableNames := make([]string, len(m.tables))

	i := 0
	for k := range m.tables {
		tableNames[i] = k
		i++
	}
	return tableNames
}
func (m *LmnInMemDataSourcev2) Table(tableName string) (*schema.Table, error) {
	if val, ok := m.tables[tableName]; ok {
		return val.table, nil
	}
	return nil, schema.ErrNotFound
}

func (m *LmnInMemDataSourcev2) Open(connInfo string) (schema.Conn, error) {
	// it is already opened - fix later
	if val, ok := m.tables[connInfo]; ok {
		return val, nil
	}
	return nil, schema.ErrNotFound
}

func (m *LmnInMemDataSourcev2) Close() error {
	m.tables = make(map[string]*LmnInMemTablev2)
	return nil
}

func (m *LmnInMemTablev2) Close() error {
	m.rowCount = 0
	return nil
}

func (m *LmnInMemTablev2) Columns() []string {
	return m.headers
}

func (m *LmnInMemTablev2) getCellValue(col *arrow.Column, rowIndex int) interface{} {
	if col == nil || rowIndex < 0 || rowIndex >= col.Len() {
		return nil
	}
	// Walk the chunks until we find the one containing rowIndex.
	for _, chunk := range col.Data().Chunks() {
		n := chunk.Len()
		if rowIndex < n {
			return arrayValue(chunk, rowIndex)
		}
		rowIndex -= n
	}
	return nil
}

// arrayValue reads slot i of arr as the native Go type qlbridge's value.NewValue
// expects for the column type we declared in AddTable: civil.Date/civil.Time for
// the temporal-only types and a map for objects (stored as JSON text).
func arrayValue(arr arrow.Array, i int) interface{} {
	if arr == nil || i < 0 || i >= arr.Len() || arr.IsNull(i) {
		return nil
	}
	switch a := arr.(type) {
	case *array.Int32:
		return a.Value(i)
	case *array.Int64:
		return a.Value(i)
	case *array.Float64:
		return a.Value(i)
	case *array.Boolean:
		return a.Value(i)
	case *array.String:
		return a.Value(i)
	case *array.LargeString:
		// object columns are stored as JSON text - hand back the decoded map so
		// qlbridge builds a MapValue, matching the MapValueType field we declared
		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(a.Value(i)), &obj); err != nil {
			return a.Value(i)
		}
		return obj
	case *array.Timestamp:
		unit := a.DataType().(*arrow.TimestampType).Unit
		return a.Value(i).ToTime(unit)
	case *array.Date32:
		return civil.DateOf(a.Value(i).ToTime())
	case *array.Time64:
		unit := a.DataType().(*arrow.Time64Type).Unit
		return civil.TimeOf(a.Value(i).ToTime(unit))
	default:
		return nil
	}
}

func (m *LmnInMemTablev2) Next() schema.Message {
	select {
	case <-m.exit:
		return nil
	default:
		for {
			if m.rowCount == uint64(m.dataset.NumRows()) {
				return nil
			}
			rowIndex := int(m.rowCount % uint64(m.dataset.NumRows()))
			m.rowCount++
			vals := make([]driver.Value, m.dataset.NumCols())
			for i := range m.dataset.NumCols() {
				vals[i] = m.getCellValue(m.dataset.Column(int(i)), rowIndex)
			}
			return &datasource.SqlDriverMessageMap{IdVal: m.rowCount, ColIndex: m.colindex, Vals: vals}
		}
	}
}
