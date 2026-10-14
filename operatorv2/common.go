package operatorv2

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"math/rand"

	"cloud.google.com/go/civil"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/araddon/qlbridge/expr/builtins"
	"github.com/araddon/qlbridge/lex"
	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
	"github.com/liminaab/filtrify/conversion"
	"github.com/liminaab/filtrify/lmnqlbridge"
	"github.com/liminaab/filtrify/types"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyz123456789"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

// src is shared process-wide. math/rand.Source is NOT safe for concurrent use,
// so every read must hold srcMu — otherwise concurrent executeSQLQuery callers
// (the REST and gRPC transform endpoints run on separate goroutines) race on its
// internal state and can produce identical/correlated "random" schema names.
var (
	srcMu sync.Mutex
	src   = rand.NewSource(time.Now().UnixNano())

	// schemaNameSeq makes every executeSQLQuery schema name globally unique even
	// if the random suffix ever repeats. All callers share the same global schema
	// registry, so a duplicate name lets one call's deferred SchemaDrop tear down
	// the schema another call is still querying — which silently returns an empty
	// result set for the in-memory query (e.g. a derived column comes back blank).
	schemaNameSeq uint64

	// registryMu serialises the register -> query -> drop sequence in
	// executeSQLQuery. qlbridge's schema registry (schema.DefaultRegistry) is a
	// single process-global structure that is NOT safe for concurrent schema
	// mutation vs. query: one query's SchemaDrop races another's table lookup over
	// the shared database list. Unique schema names alone don't fix that, so the
	// whole critical section must run under this lock.
	registryMu sync.Mutex
)

const defaultTableName string = "ext"

func init() {
	builtins.LoadAllBuiltins()
	lmnqlbridge.LoadLiminaOperators()
}

func ParseTime(s string) (time.Time, error) {
	t, err := time.Parse("15:04:05.999999999", s)
	if err == nil {
		return t, nil
	}
	t, err = time.Parse("15:04:05", s)
	if err == nil {
		return t, nil
	}
	t, err = time.Parse("15:04", s)
	if err == nil {
		return t, nil
	}
	return time.Time{}, err
}

func extractHeadersAndTypeMap(dataset *types.DataSetv2) ([]string, map[string]types.CellDataType) {
	if dataset.NumRows() == 0 {
		return []string{}, make(map[string]types.CellDataType)
	}
	columnTypeMap := make(map[string]types.CellDataType)
	columnLength := dataset.NumCols()
	cols := make([]string, columnLength)
	for i := range columnLength {
		theCol := dataset.Column(int(i))
		cols[i] = theCol.Name()
		columnTypeMap[cols[i]] = conversion.FiltrifyType(theCol.DataType())
	}

	return cols, columnTypeMap
}

func buildSelectStatement(selectFields []string) string {
	var sb strings.Builder
	for i, h := range selectFields {
		sb.WriteString(fmt.Sprintf("`%s`", h))
		if i != len(selectFields)-1 {
			sb.WriteString(",")
		}
	}
	return sb.String()
}

// sourceValue reads slot i of arr as the native Go type the conversionFuncs
// expect. GetOneForMarshal is not usable here - it renders temporal values as
// JSON strings, while the conversionFuncs assert on time.Time.
func sourceValue(arr arrow.Array, i int) (interface{}, error) {
	switch a := arr.(type) {
	case *array.Int32:
		return a.Value(i), nil
	case *array.Int64:
		return a.Value(i), nil
	case *array.Float64:
		return a.Value(i), nil
	case *array.Boolean:
		return a.Value(i), nil
	case *array.String:
		return a.Value(i), nil
	case *array.LargeString:
		return a.Value(i), nil
	case *array.Timestamp:
		unit := a.DataType().(*arrow.TimestampType).Unit
		return a.Value(i).ToTime(unit), nil
	case *array.Date32:
		return a.Value(i).ToTime(), nil
	case *array.Time64:
		unit := a.DataType().(*arrow.Time64Type).Unit
		return a.Value(i).ToTime(unit), nil
	default:
		return nil, fmt.Errorf("unsupported source array %T", arr)
	}
}

func RandStringBytesMaskImprSrcUnsafe(n int) string {
	b := make([]byte, n)
	// math/rand.Source is not concurrency-safe; serialise access to the shared src.
	srcMu.Lock()
	defer srcMu.Unlock()
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return *(*string)(unsafe.Pointer(&b))
}

func executeSQLQuery(q string, dataset *types.DataSetv2, existingColumnTypeMap map[string]types.CellDataType) (*types.DataSetv2, error) {
	exit := make(chan bool)
	inMemoryDataSource := lmnqlbridge.NewLmnInMemDataSourcev2(exit)

	inMemoryDataSource.AddTable(defaultTableName, dataset)
	// Suffix with a process-global sequence so the name is unique even on a random
	// collision; concurrent calls share the global schema registry and a reused
	// name causes cross-call SchemaDrop interference (empty query results).
	schemaName := RandStringBytesMaskImprSrcUnsafe(15) + strconv.FormatUint(atomic.AddUint64(&schemaNameSeq, 1), 36)

	// Serialise the register -> query -> drop sequence: qlbridge's global schema
	// registry is not concurrency-safe (see registryMu). Unlock runs after the
	// deferred SchemaDrop below (defers are LIFO and this one is registered first).
	registryMu.Lock()
	defer registryMu.Unlock()

	err := schema.RegisterSourceAsSchema(schemaName, inMemoryDataSource)
	if err != nil {
		return nil, err
	}
	defer func() {
		schema.DefaultRegistry().SchemaDrop(schemaName, schemaName, lex.TokenSchema)
	}()
	result, columns, err := lmnqlbridge.RunQLQuery(schemaName, q)
	if err != nil {
		return nil, err
	}
	ds := convertToDataSet(result, columns, existingColumnTypeMap)
	return ds, nil
}

// derefValue unwraps the pointer RunQLQuery's reflect-based scan leaves behind
// and returns the value underneath it.
func derefValue(d interface{}) interface{} {
	for d != nil {
		rv := reflect.ValueOf(d)
		switch rv.Kind() {
		case reflect.Ptr, reflect.Interface:
			if rv.IsNil() {
				return nil
			}
			d = rv.Elem().Interface()
		default:
			return d
		}
	}
	return nil
}

// queryCellValue maps one scanned query value onto the filtrify type the
// pre-arrow convertToCell used to produce, together with the native value the
// matching arrow builder expects. A nil value means "no value" - the caller
// leaves a null behind. existingType disambiguates time.Time, which could be a
// timestamp, a date or a time of day.
func queryCellValue(d interface{}, existingType types.CellDataType) (types.CellDataType, interface{}) {
	switch v := derefValue(d).(type) {
	case int32:
		return types.IntType, v
	case int64:
		return types.LongType, v
	case int:
		return types.LongType, int64(v)
	case civil.Date:
		return types.DateType, v.In(time.UTC)
	case civil.Time:
		return types.TimeOfDayType, time.Date(0, 0, 0, v.Hour, v.Minute, v.Second, v.Nanosecond, time.UTC)
	case time.Time:
		// this might be 3 different types - we need the existing column type to
		// tell them apart, and assume a timestamp when we don't have one
		switch existingType {
		case types.TimestampType, types.DateType, types.TimeOfDayType:
			return existingType, v
		}
		return types.TimestampType, v
	case string:
		return types.StringType, v
	case float32:
		return types.DoubleType, float64(v)
	case float64:
		return types.DoubleType, v
	case bool:
		return types.BoolType, v
	case map[string]value.Value:
		// let's convert this to our internal object type
		objectVal := make(map[string]interface{}, len(v))
		for k, item := range v {
			objectVal[k] = item.Value()
		}
		// object columns are stored as JSON text
		b, err := json.Marshal(objectVal)
		if err != nil {
			return types.NilType, nil
		}
		return types.ObjectType, string(b)
	default:
		return types.NilType, nil
	}
}

func convertToDataSet(data [][]interface{}, headers []string, existingColumnTypeMap map[string]types.CellDataType) *types.DataSetv2 {
	existingTypes := make([]types.CellDataType, len(headers))
	for i, h := range headers {
		existingType, ok := existingColumnTypeMap[h]
		if !ok {
			existingType = types.NilType
		}
		existingTypes[i] = existingType
	}

	// arrow needs a single type per column, so the first row carrying a value
	// decides - every cell of a column comes back from the same scan type.
	// Columns with nothing but nils stay null typed.
	colTypes := make([]types.CellDataType, len(headers))
	for i := range headers {
		colTypes[i] = types.NilType
		for _, r := range data {
			if i >= len(r) {
				continue
			}
			t, _ := queryCellValue(r[i], existingTypes[i])
			if t != types.NilType {
				colTypes[i] = t
				break
			}
		}
	}

	fields := make([]arrow.Field, len(headers))
	for i, h := range headers {
		fields[i] = arrow.Field{
			Name:     h,
			Type:     conversion.ArrowType(colTypes[i]),
			Nullable: true,
		}
	}

	dataSchema := arrow.NewSchema(fields, nil)
	rowBuilder := array.NewRecordBuilder(memory.DefaultAllocator, dataSchema)
	defer rowBuilder.Release()
	rowBuilder.Reserve(len(data))

	for _, r := range data {
		for i := range fields {
			arrayBuilder := rowBuilder.Field(i)
			if i >= len(r) {
				arrayBuilder.AppendNull()
				continue
			}
			_, val := queryCellValue(r[i], existingTypes[i])
			if val == nil {
				arrayBuilder.AppendNull()
				continue
			}
			if err := appendConverted(arrayBuilder, val); err != nil {
				arrayBuilder.AppendNull()
			}
		}
	}

	rec := rowBuilder.NewRecordBatch()
	defer rec.Release()
	return &types.DataSetv2{
		array.NewTableFromRecords(dataSchema, []arrow.RecordBatch{rec}),
	}
}

func buildColumnNotExistsError(column string) error {
	return fmt.Errorf("attempted to operate on column “%s” but no such column available", column)
}

/*
func addKeyRowToDataset(headers []string, colTypeMap map[string]types.CellDataType, dataset *types.DataSetv2) ([]string, map[string]types.CellDataType, *types.DataSetv2) {
	headers = append(headers, liminaKeyColumn)
	if dataset.Headers != nil {
		dataset.Headers[liminaKeyColumn] = &types.Header{
			ColumnName: liminaKeyColumn,
			DataType:   types.StringType,
		}
	}
	// let's append rowKey to each row
	for i, r := range dataset.Rows {
		dataset.Rows[i].Columns = append(r.Columns, types.NewStringDataColumn(r.Key, liminaKeyColumn))
	}
	colTypeMap[liminaKeyColumn] = types.StringType
	return headers, colTypeMap, dataset
}
*/
