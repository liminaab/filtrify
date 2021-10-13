package lmnqlbridge

import (
	"database/sql/driver"
	"strings"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
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

type LmnInMemRawTable struct {
	table    *schema.Table
	dataset  [][]string
	headers  []string
	exit     <-chan bool
	rowCount uint64
	colindex map[string]int
}

type LmnInMemRawDataSource struct {
	tables map[string]*LmnInMemRawTable
	exit   <-chan bool
}

// NewCsvSource reader assumes we are getting first row as headers
// - optionally may be gzipped
func NewLmnInMemRawDataSource(exit <-chan bool) *LmnInMemRawDataSource {
	m := LmnInMemRawDataSource{tables: make(map[string]*LmnInMemRawTable)}
	return &m
}

func (m *LmnInMemRawDataSource) ReplaceTable(name string, headers []string, dataset [][]string) {
	delete(m.tables, name)
	m.AddTable(name, headers, dataset)
}

func (m *LmnInMemRawDataSource) AddTable(name string, headers []string, dataset [][]string) {
	inMemTable := &LmnInMemRawTable{
		headers:  headers,
		dataset:  dataset,
		exit:     m.exit,
		rowCount: 0,
	}
	inMemTable.table = schema.NewTable(strings.ToLower(name))
	colindex := make(map[string]int, len(headers))
	for i := range headers {
		headers[i] = strings.ToLower(headers[i])
		colindex[headers[i]] = i
		inMemTable.table.AddField(schema.NewFieldBase(headers[i], value.StringType, 64, "string"))
	}
	inMemTable.colindex = colindex
	inMemTable.dataset = dataset
	m.tables[name] = inMemTable
}

func (m *LmnInMemRawDataSource) Init()                      {}
func (m *LmnInMemRawDataSource) Setup(*schema.Schema) error { return nil }
func (m *LmnInMemRawDataSource) Tables() []string {
	tableNames := make([]string, len(m.tables))

	i := 0
	for k := range m.tables {
		tableNames[i] = k
		i++
	}
	return tableNames
}
func (m *LmnInMemRawDataSource) Table(tableName string) (*schema.Table, error) {
	if val, ok := m.tables[tableName]; ok {
		return val.table, nil
	}
	return nil, schema.ErrNotFound
}

func (m *LmnInMemRawDataSource) Open(connInfo string) (schema.Conn, error) {
	// it is already opened - fix later
	if val, ok := m.tables[connInfo]; ok {
		return val, nil
	}
	return nil, schema.ErrNotFound
}

func (m *LmnInMemRawDataSource) Close() error {
	// TODO handle this
	return nil
}

func (m *LmnInMemRawTable) Close() error {
	// TODO close file
	return nil
}

func (m *LmnInMemRawTable) Columns() []string {
	// TODO close file
	return m.headers
}

func (m *LmnInMemRawTable) Next() schema.Message {
	select {
	case <-m.exit:
		return nil
	default:
		for {
			if m.rowCount == uint64(len(m.dataset)) {
				return nil
			}
			row := m.dataset[m.rowCount]
			m.rowCount++
			if len(row) != len(m.headers) {
				u.Warnf("headers/cols dont match, dropping expected:%d got:%d vals=%v", len(m.headers), len(row), row)
				continue
			}
			vals := make([]driver.Value, len(row))
			for i, val := range row {
				vals[i] = val
			}
			//u.Debugf("headers: %#v \n\trows:  %#v", m.headers, row)
			// return datasource.NewSqlDriverMessageMap(m.rowCount, , m.colindex)
			mm := &datasource.SqlDriverMessageMap{IdVal: m.rowCount, ColIndex: m.colindex, Vals: vals}
			// TODO fix this !!!
			switch v := vals[0].(type) {
			case string:
				mm.SetKey(v)
			}

			return mm
		}
	}
}
