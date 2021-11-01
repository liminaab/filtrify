package lmnqlbridge

import (
	"database/sql/driver"
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
	"limina.com/dyntransformer/types"
)

// var (
// 	_ schema.Source      = (*LmnCSVDataSource)(nil)
// 	_ schema.Conn        = (*LmnCSVDataSource)(nil)
// 	_ schema.ConnScanner = (*LmnCSVDataSource)(nil)
// )

// Csv DataSource, implements qlbridge schema DataSource, SourceConn, Scanner
//   to allow csv files to be full featured databases.
//   - very, very naive scanner, forward only single pass
//   - can open a file with .Open()
//   - assumes comma delimited
//   - not thread-safe
//   - does not implement write operations

type LmnInMemTable struct {
	table    *schema.Table
	dataset  *types.DataSet
	headers  []string
	exit     <-chan bool
	rowCount uint64
	colindex map[string]int
}

type LmnInMemDataSource struct {
	tables map[string]*LmnInMemTable
	exit   <-chan bool
}

// NewCsvSource reader assumes we are getting first row as headers
// - optionally may be gzipped
func NewLmnInMemDataSource(exit <-chan bool) *LmnInMemDataSource {
	m := LmnInMemDataSource{tables: make(map[string]*LmnInMemTable)}
	return &m
}

func (m *LmnInMemDataSource) ReplaceTable(name string, dataset *types.DataSet) {
	delete(m.tables, name)
	m.AddTable(name, dataset)
}

func (m *LmnInMemDataSource) mapToInternalType(t types.CellDataType) value.ValueType {
	switch t {
	case types.TimestampType:
		return value.TimeType
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
	}

	return value.NilType
}

func (m *LmnInMemDataSource) AddTable(name string, dataset *types.DataSet) {
	inMemTable := &LmnInMemTable{
		dataset:  dataset,
		exit:     m.exit,
		rowCount: 0,
	}

	inMemTable.table = schema.NewTable(strings.ToLower(name))
	sampleRow := dataset.Rows[0]
	inMemTable.headers = make([]string, len(sampleRow.Columns))
	// let's calculate headers
	for i, col := range sampleRow.Columns {
		inMemTable.headers[i] = col.ColumnName
	}
	colindex := make(map[string]int, len(sampleRow.Columns))
	for i := range sampleRow.Columns {
		colindex[sampleRow.Columns[i].ColumnName] = i
		internalType := m.mapToInternalType(sampleRow.Columns[i].CellValue.DataType)
		// TODO calculate length !!!!!!!!!!!!!!!!!!!
		inMemTable.table.AddField(schema.NewFieldBase(sampleRow.Columns[i].ColumnName, internalType, 64, sampleRow.Columns[i].ColumnName))
	}
	inMemTable.colindex = colindex
	inMemTable.dataset = dataset
	m.tables[name] = inMemTable
}

func (m *LmnInMemDataSource) Init()                      {}
func (m *LmnInMemDataSource) Setup(*schema.Schema) error { return nil }
func (m *LmnInMemDataSource) Tables() []string {
	tableNames := make([]string, len(m.tables))

	i := 0
	for k := range m.tables {
		tableNames[i] = k
		i++
	}
	return tableNames
}
func (m *LmnInMemDataSource) Table(tableName string) (*schema.Table, error) {
	if val, ok := m.tables[tableName]; ok {
		return val.table, nil
	}
	return nil, schema.ErrNotFound
}

func (m *LmnInMemDataSource) Open(connInfo string) (schema.Conn, error) {
	// it is already opened - fix later
	if val, ok := m.tables[connInfo]; ok {
		return val, nil
	}
	return nil, schema.ErrNotFound
}

func (m *LmnInMemDataSource) Close() error {
	m.tables = make(map[string]*LmnInMemTable)
	return nil
}

func (m *LmnInMemTable) Close() error {
	m.rowCount = 0
	return nil
}

func (m *LmnInMemTable) Columns() []string {
	return m.headers
}

func (m *LmnInMemTable) getCellValue(col *types.DataColumn) interface{} {
	switch col.CellValue.DataType {
	case types.TimestampType:
		return col.CellValue.TimestampValue
	case types.IntType:
		return col.CellValue.IntValue
	case types.LongType:
		return col.CellValue.LongValue
	case types.DoubleType:
		return col.CellValue.DoubleValue
	case types.BoolType:
		return col.CellValue.BoolValue
	case types.StringType:
		return col.CellValue.StringValue
	case types.NilType:
		return nil
	}

	return nil
}

func (m *LmnInMemTable) Next() schema.Message {
	select {
	case <-m.exit:
		return nil
	default:
		for {
			if m.rowCount == uint64(len(m.dataset.Rows)) {
				return nil
			}
			row := m.dataset.Rows[m.rowCount]
			m.rowCount++
			if len(row.Columns) != len(m.headers) {
				u.Warnf("headers/cols dont match, dropping expected:%d got:%d vals=%v", len(m.headers), len(row.Columns), row)
				continue
			}
			vals := make([]driver.Value, len(row.Columns))
			for i, val := range row.Columns {
				vals[i] = m.getCellValue(val)
			}
			//u.Debugf("headers: %#v \n\trows:  %#v", m.headers, row)
			// return datasource.NewSqlDriverMessageMap(m.rowCount, , m.colindex)
			mm := &datasource.SqlDriverMessageMap{IdVal: m.rowCount, ColIndex: m.colindex, Vals: vals}
			// TODO fix this !!!!!!!!
			switch v := vals[0].(type) {
			case string:
				mm.SetKey(v)
			}

			return mm
		}
	}
}
