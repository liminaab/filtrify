package operatorv2

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/liminaab/filtrify/types"
)

type LookupOperator struct {
}

type JoinColumn struct {
	Left  string `json:"left"`
	Right string `json:"right"`
}

type LookupFilter struct {
	Value  string `json:"value"`
	Filter string `json:"filter"`
}

type LookupConfiguration struct {
	TargetDataset            string                  `json:"targetDataset"`
	Columns                  []*JoinColumn           `json:"columns"`
	RemoveRightMatchColumn   bool                    `json:"removeRightMatchColumn"`
	RemoveRightDatasetPrefix bool                    `json:"removeRightDatasetPrefix"`
	SelectedColumns          []string                `json:"selectedColumns"`
	TargetDatasetFilters     map[string]LookupFilter `json:"targetDatasetFilters"`
}

// flatColumn gives random access to a chunked column by absolute row index.
// A table built by one of the operators normally holds a single chunk, but
// nothing guarantees that, so every read goes through here.
type flatColumn struct {
	chunks []arrow.Array
	starts []int64
}

func newFlatColumn(col *arrow.Column) *flatColumn {
	chunks := col.Data().Chunks()
	starts := make([]int64, len(chunks))
	var offset int64
	for i, c := range chunks {
		starts[i] = offset
		offset += int64(c.Len())
	}
	return &flatColumn{chunks: chunks, starts: starts}
}

func (f *flatColumn) at(row int64) (arrow.Array, int, bool) {
	for i := len(f.chunks) - 1; i >= 0; i-- {
		if row < f.starts[i] {
			continue
		}
		idx := int(row - f.starts[i])
		if idx >= f.chunks[i].Len() {
			return nil, 0, false
		}
		return f.chunks[i], idx, true
	}
	return nil, 0, false
}

// appendArrayValue copies slot i of arr into b. Both must have the same type -
// every call site builds b out of the source column's own data type.
func appendArrayValue(b array.Builder, arr arrow.Array, i int) error {
	if arr.IsNull(i) {
		b.AppendNull()
		return nil
	}
	switch a := arr.(type) {
	case *array.Int32:
		bb, ok := b.(*array.Int32Builder)
		if !ok {
			return fmt.Errorf("expected an int32 builder, got %T", b)
		}
		bb.Append(a.Value(i))
	case *array.Int64:
		bb, ok := b.(*array.Int64Builder)
		if !ok {
			return fmt.Errorf("expected an int64 builder, got %T", b)
		}
		bb.Append(a.Value(i))
	case *array.Float64:
		bb, ok := b.(*array.Float64Builder)
		if !ok {
			return fmt.Errorf("expected a float64 builder, got %T", b)
		}
		bb.Append(a.Value(i))
	case *array.Boolean:
		bb, ok := b.(*array.BooleanBuilder)
		if !ok {
			return fmt.Errorf("expected a boolean builder, got %T", b)
		}
		bb.Append(a.Value(i))
	case *array.String:
		bb, ok := b.(*array.StringBuilder)
		if !ok {
			return fmt.Errorf("expected a string builder, got %T", b)
		}
		bb.Append(a.Value(i))
	case *array.LargeString:
		bb, ok := b.(*array.LargeStringBuilder)
		if !ok {
			return fmt.Errorf("expected a large string builder, got %T", b)
		}
		bb.Append(a.Value(i))
	case *array.Timestamp:
		bb, ok := b.(*array.TimestampBuilder)
		if !ok {
			return fmt.Errorf("expected a timestamp builder, got %T", b)
		}
		bb.Append(a.Value(i))
	case *array.Date32:
		bb, ok := b.(*array.Date32Builder)
		if !ok {
			return fmt.Errorf("expected a date32 builder, got %T", b)
		}
		bb.Append(a.Value(i))
	case *array.Time64:
		bb, ok := b.(*array.Time64Builder)
		if !ok {
			return fmt.Errorf("expected a time64 builder, got %T", b)
		}
		bb.Append(a.Value(i))
	case *array.Null:
		b.AppendNull()
	default:
		return fmt.Errorf("unsupported source array %T", arr)
	}
	return nil
}

func (t *LookupOperator) isRightMatchColumn(colName string, config *LookupConfiguration) bool {
	for _, c := range config.Columns {
		if colName == c.Right {
			return true
		}
	}
	return false
}

func (t *LookupOperator) isSelectedColumn(colName string, config *LookupConfiguration) bool {
	for _, sc := range config.SelectedColumns {
		if strings.EqualFold(sc, colName) {
			return true
		}
	}
	return false
}

func (t *LookupOperator) getRightColumnName(orgDataset *types.DataSetv2, colName string, config *LookupConfiguration) string {
	var columnName string
	if config.RemoveRightDatasetPrefix {
		columnName = colName
	} else {
		columnName = fmt.Sprintf("%s.%s", config.TargetDataset, colName)
	}
	counter := 1
	for {
		exists := false
		// let's check if this name already exists
		for i := range orgDataset.NumCols() {
			if orgDataset.Column(int(i)).Name() == columnName {
				exists = true
				break
			}
		}
		if exists {
			columnName = fmt.Sprintf("%s_%d", columnName, counter)
			counter++
		} else {
			break
		}
	}
	return columnName
}

// joinKeysOf builds a composite lookup key out of every row's join columns,
// matching EqualsAsText's notion of equality closely enough for the common case
// (same-type or cross-type-but-textually-equal values). Each part is
// length-prefixed so that e.g. ["ab","c"] can never collide with ["a","bc"]. A
// null (or missing) value makes the row unmatchable, mirroring Equals() which
// always returns false for NilType even when comparing nil to nil. The second
// result reports, per row, whether a key could be built at all.
func (t *LookupOperator) joinKeysOf(ds *types.DataSetv2, names []string) ([]string, []bool, error) {
	cols := make([]*flatColumn, len(names))
	for i, name := range names {
		col := ds.GetColumn(name)
		if col == nil {
			return nil, nil, buildColumnNotExistsError(name)
		}
		cols[i] = newFlatColumn(col)
	}

	keys := make([]string, ds.NumRows())
	usable := make([]bool, ds.NumRows())
	for row := int64(0); row < ds.NumRows(); row++ {
		var sb strings.Builder
		complete := true
		for _, fc := range cols {
			arr, idx, found := fc.at(row)
			if !found {
				complete = false
				break
			}
			part, ok := t.canonicalCellText(arr, idx)
			if !ok {
				complete = false
				break
			}
			sb.WriteString(strconv.Itoa(len(part)))
			sb.WriteByte(':')
			sb.WriteString(part)
		}
		if !complete {
			continue
		}
		keys[row] = sb.String()
		usable[row] = true
	}
	return keys, usable, nil
}

// canonicalCellText renders slot i of arr to text for hashing. It matches
// CellValue.ToString() except for timestamp-like types, which are normalized to
// UTC with nanosecond precision so two representations of the same instant in
// different locations hash identically - ToString()'s RFC3339 (no sub-second
// digits) can otherwise collide distinct instants or split equal ones across
// zones. A null reports false: the row is unmatchable.
func (t *LookupOperator) canonicalCellText(arr arrow.Array, i int) (string, bool) {
	if arr.IsNull(i) {
		return "", false
	}
	switch a := arr.(type) {
	case *array.Null:
		return "", false
	case *array.Int32:
		return strconv.FormatInt(int64(a.Value(i)), 10), true
	case *array.Int64:
		return strconv.FormatInt(a.Value(i), 10), true
	case *array.Float64:
		return strconv.FormatFloat(a.Value(i), 'f', -1, 64), true
	case *array.Boolean:
		if a.Value(i) {
			return "true", true
		}
		return "false", true
	case *array.String:
		return a.Value(i), true
	case *array.LargeString:
		// object columns are stored as JSON text
		return a.Value(i), true
	case *array.Timestamp:
		unit := a.DataType().(*arrow.TimestampType).Unit
		return a.Value(i).ToTime(unit).UTC().Format(time.RFC3339Nano), true
	case *array.Date32:
		return a.Value(i).ToTime().UTC().Format(time.RFC3339Nano), true
	case *array.Time64:
		unit := a.DataType().(*arrow.Time64Type).Unit
		tod := a.Value(i).ToTime(unit).UTC()
		// a time of day carries no date - pin it to the same zero day the v1
		// parser used so the rendered text only depends on the clock
		return time.Date(0, 1, 1, tod.Hour(), tod.Minute(), tod.Second(), tod.Nanosecond(), time.UTC).Format(time.RFC3339Nano), true
	default:
		return arr.ValueStr(i), true
	}
}

// gatherRows builds a column whose i-th row is row rows[i] of src, keeping the
// given field. A row outside src leaves a null behind. The returned column is an
// owned reference - the caller releases it.
func gatherRows(src *arrow.Column, field arrow.Field, rows []int64) (*arrow.Column, error) {
	flat := newFlatColumn(src)
	builder := array.NewBuilder(memory.DefaultAllocator, src.DataType())
	defer builder.Release()
	builder.Reserve(len(rows))

	for _, r := range rows {
		arr, idx, found := flat.at(r)
		if !found {
			builder.AppendNull()
			continue
		}
		if err := appendArrayValue(builder, arr, idx); err != nil {
			return nil, err
		}
	}

	arr := builder.NewArray()
	defer arr.Release()

	chunked := arrow.NewChunked(src.DataType(), []arrow.Array{arr}) // retains arr
	defer chunked.Release()
	return arrow.NewColumn(field, chunked), nil // retains chunked
}

// gatherColumn builds one joined column: row i of the result takes row
// matches[i] of src, or a null when matches[i] is negative (no match) - an out
// of range row has nothing to copy, which is exactly how gatherRows treats it.
// The returned column is an owned reference - the caller releases it.
func (t *LookupOperator) gatherColumn(src *arrow.Column, name string, matches []int64) (*arrow.Column, error) {
	oldField := src.Field()
	field := arrow.Field{
		Name:     name,
		Type:     src.DataType(),
		Nullable: true,
		Metadata: oldField.Metadata,
	}
	return gatherRows(src, field, matches)
}

// placeholderColumn builds an all-null column of the null type. It stands in for
// the columns an invalid configuration promised but never produced, mirroring
// the "column_%d" filler the row based implementation left behind.
func placeholderColumn(name string, rows int64) *arrow.Column {
	builder := array.NewNullBuilder(memory.DefaultAllocator)
	defer builder.Release()
	for range rows {
		builder.AppendNull()
	}
	arr := builder.NewArray()
	defer arr.Release()

	field := arrow.Field{Name: name, Type: arrow.Null, Nullable: true}
	chunked := arrow.NewChunked(field.Type, []arrow.Array{arr}) // retains arr
	defer chunked.Release()
	return arrow.NewColumn(field, chunked) // retains chunked
}

func (t *LookupOperator) mergeSets(left *types.DataSetv2, right *types.DataSetv2, config *LookupConfiguration) (*types.DataSetv2, error) {
	if right.NumRows() == 0 {
		// no data on right - we can't merge it
		// TODO maybe create empty columns for right?
		return left, nil
	}

	rightNames := make([]string, len(config.Columns))
	leftNames := make([]string, len(config.Columns))
	for i, jc := range config.Columns {
		rightNames[i] = jc.Right
		leftNames[i] = jc.Left
	}

	rightKeys, rightUsable, err := t.joinKeysOf(right, rightNames)
	if err != nil {
		return nil, err
	}
	leftKeys, leftUsable, err := t.joinKeysOf(left, leftNames)
	if err != nil {
		return nil, err
	}

	// index the right side once by join key instead of rescanning it for every left row -
	// this is what turns the join from O(len(left)*len(right)) into O(len(left)+len(right)).
	// Only the first row per key is kept, matching the original loop's "first match wins" order.
	rightByKey := make(map[string]int64, len(rightKeys))
	for row, key := range rightKeys {
		if !rightUsable[row] {
			continue
		}
		if _, exists := rightByKey[key]; !exists {
			rightByKey[key] = int64(row)
		}
	}

	// matches[i] is the right row left row i joins to, or -1 for a nil merge
	matches := make([]int64, left.NumRows())
	for row := range matches {
		matches[row] = -1
		if !leftUsable[row] {
			continue
		}
		if rightRow, found := rightByKey[leftKeys[row]]; found {
			matches[row] = rightRow
		}
	}

	// how many columns the right side contributes
	targetWidth := int(right.NumCols())
	if config.RemoveRightMatchColumn {
		targetWidth -= len(config.Columns)
	}
	if len(config.SelectedColumns) > 0 {
		targetWidth = len(config.SelectedColumns)
	}
	if targetWidth < 0 {
		return nil, errors.New("invalid lookup configuration")
	}

	newColumns := make([]arrow.Column, 0, int(left.NumCols())+targetWidth)
	// every entry of newColumns is a reference we own - NewTable takes its own,
	// so we hand ours back once we are done (or on the way out if we fail)
	defer func() {
		for i := range newColumns {
			newColumns[i].Release()
		}
	}()

	for i := range left.NumCols() {
		col := left.Column(int(i))
		col.Retain() // we release it below, so take a ref of our own
		newColumns = append(newColumns, *col)
	}

	produced := 0
	for i := range right.NumCols() {
		rightCol := right.Column(int(i))
		if config.RemoveRightMatchColumn && t.isRightMatchColumn(rightCol.Name(), config) {
			// let's skip this
			continue
		}
		if len(config.SelectedColumns) > 0 && !t.isSelectedColumn(rightCol.Name(), config) {
			// let's check if this column is selected
			continue
		}
		if produced >= targetWidth {
			// the configuration promised fewer columns than the right side yields -
			// e.g. a join column repeated in config.Columns
			return nil, errors.New("invalid lookup configuration")
		}
		joinedCol, err := t.gatherColumn(rightCol, t.getRightColumnName(left, rightCol.Name(), config), matches)
		if err != nil {
			return nil, err
		}
		newColumns = append(newColumns, *joinedCol)
		produced++
	}

	// if the configuration is invalid - some columns might be missing
	// let's fill them with nil columns
	for i := int(left.NumCols()) + produced; i < int(left.NumCols())+targetWidth; i++ {
		newColumns = append(newColumns, *placeholderColumn(fmt.Sprintf("column_%d", i), left.NumRows()))
	}

	fields := make([]arrow.Field, len(newColumns))
	for i := range newColumns {
		fields[i] = newColumns[i].Field()
	}
	oldMetadata := left.Schema().Metadata()
	schema := arrow.NewSchema(fields, &oldMetadata)
	mergedTable := array.NewTable(schema, newColumns, left.NumRows()) // retains each column
	return &types.DataSetv2{
		mergedTable,
	}, nil
}

func (t *LookupOperator) Transform(dataset *types.DataSetv2, config string, otherSets map[string]*types.DataSetv2) (*types.DataSetv2, error) {

	typedConfig, err := t.buildConfiguration(config)
	if err != nil {
		return nil, err
	}

	if _, ok := otherSets[typedConfig.TargetDataset]; !ok {
		return nil, errors.New("target dataset not found")
	}

	tds := otherSets[typedConfig.TargetDataset]

	if tds.NumRows() < 1 || dataset.NumRows() < 1 {
		return dataset, nil
	}

	// let's check if columns exist
	for _, col := range typedConfig.Columns {
		realCol := tds.GetColumn(col.Right)
		if realCol == nil {
			return nil, buildColumnNotExistsError(col.Right)
		}
	}

	for _, col := range typedConfig.Columns {
		realCol := dataset.GetColumn(col.Left)
		if realCol == nil {
			return nil, buildColumnNotExistsError(col.Left)
		}
	}

	filteredSet := tds
	if len(typedConfig.TargetDatasetFilters) > 0 {
		filterOp := &FilterOperator{}
		// before merging the data - let's do a filter for the target dataset if it exists
		for _, filter := range typedConfig.TargetDatasetFilters {
			if len(filter.Filter) == 0 {
				continue
			}
			filteredSet, err = filterOp.Transform(filteredSet, filter.Filter, nil)
			if err != nil {
				return nil, err
			}
		}
	}

	// wow we are ready to join those tables
	return t.mergeSets(dataset, filteredSet, typedConfig)
}

func (t *LookupOperator) buildConfiguration(config string) (*LookupConfiguration, error) {
	if len(config) < 1 {
		return nil, errors.New("invalid configuration")
	}
	// config is a json declaration of our field configuration
	typedConfig := LookupConfiguration{}
	err := json.Unmarshal([]byte(config), &typedConfig)
	if err != nil {
		return nil, err
	}

	if len(typedConfig.TargetDataset) < 1 {
		return nil, errors.New("missing targetdataset in lookup configuration")
	}

	if len(typedConfig.Columns) < 1 {
		return nil, errors.New("missing columns in lookup configuration")
	}

	for _, ob := range typedConfig.Columns {
		if len(ob.Left) < 1 {
			return nil, errors.New("missing join left in lookup configuration")
		}
		if len(ob.Right) < 1 {
			return nil, errors.New("missing join right in lookup configuration")
		}
	}

	return &typedConfig, nil
}

func (t *LookupOperator) ValidateConfiguration(config string) (bool, error) {
	typedConfig, err := t.buildConfiguration(config)
	return typedConfig != nil, err
}
