package operatorv2

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/compute"
	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/liminaab/filtrify/types"
)

type SortOperator struct {
}

type OrderConfiguration struct {
	ColumnName string `json:"columnName"`
	Ascending  bool   `json:"ascending"`
}

type SortConfiguration struct {
	OrderBy []*OrderConfiguration `json:"orderBy"`
}

// sortableTypes are the arrow types the sort orders by value. The set mirrors
// v1's SortOperator.CompareColumns switch one for one: int, long, double, bool,
// string, and the three temporal types. Everything v1's switch has no case for
// falls out of its comparator as "equal" - an object column above all, which
// filtrify stores as a large string - so ordering by such a column must not
// reorder anything here either, however well arrow could order it.
var sortableTypes = map[arrow.Type]bool{
	arrow.INT32:     true, // IntType
	arrow.INT64:     true, // LongType
	arrow.FLOAT64:   true, // DoubleType
	arrow.BOOL:      true, // BoolType
	arrow.STRING:    true, // StringType
	arrow.TIMESTAMP: true, // TimestampType
	arrow.DATE32:    true, // DateType
	arrow.TIME64:    true, // TimeOfDayType
}

// columnIndex returns the position of the column called name, or -1 when the
// table has no such column. The sort keys address columns by index, so the name
// the configuration carries has to be resolved to one.
func columnIndex(dataset *types.DataSetv2, name string) int {
	for i := range dataset.NumCols() {
		if dataset.Column(int(i)).Name() == name {
			return int(i)
		}
	}
	return -1
}

// sortKey turns one order-by entry into the key arrow's sort takes. v1 has no
// separate notion of null placement: its comparator reports a null as smaller
// than every value and two nulls as equal, so nulls lead ascending and trail
// descending. Arrow keeps placement independent of direction, which is exactly
// what reproducing that rule needs - the placement follows the direction.
func sortKey(columnIndex int, ascending bool) compute.SortKey {
	if ascending {
		return compute.SortKey{
			ColumnIndex:   columnIndex,
			Order:         compute.SortOrderAscending,
			NullPlacement: compute.SortNullsAtStart,
		}
	}
	return compute.SortKey{
		ColumnIndex:   columnIndex,
		Order:         compute.SortOrderDescending,
		NullPlacement: compute.SortNullsAtEnd,
	}
}

func (t *SortOperator) Transform(dataset *types.DataSetv2, config string, _ map[string]*types.DataSetv2) (*types.DataSetv2, error) {
	typedConfig, confError := t.buildConfiguration(config)
	if confError != nil {
		return nil, confError
	}

	if dataset.NumRows() < 1 || dataset.NumCols() < 1 {
		// nothing to reorder - and arrow's comparator rejects a column with no
		// chunks behind it, which is what an empty table hands it
		return dataset, nil
	}

	// Keys that cannot order anything are dropped rather than passed down:
	//   - a column the table doesn't have. v1 looks it up per row, records a "no
	//     such column" error and then throws it away, returning whatever
	//     permutation its sort landed on together with a nil error. Nothing
	//     orders by such a key, so it is left out here and no error is reported
	//     either.
	//   - a column of a type v1's comparator has no case for (see sortableTypes),
	//     including a null typed one. Arrow's kernel refuses the null type
	//     outright, and would order a large string that v1 leaves alone.
	keys := make([]compute.SortKey, 0, len(typedConfig.OrderBy))
	for _, c := range typedConfig.OrderBy {
		idx := columnIndex(dataset, c.ColumnName)
		if idx < 0 {
			continue
		}
		if !sortableTypes[dataset.Column(idx).DataType().ID()] {
			continue
		}
		keys = append(keys, sortKey(idx, c.Ascending))
	}

	if len(keys) == 0 {
		// every key ties on every row, so the rows stay as they are
		return dataset, nil
	}

	// SortTable sorts the indices and takes the whole table by them, keeping the
	// schema - and with it every column's name, type and metadata. The receiver
	// is left untouched, as with the other v2 operators, and the table handed
	// back owns its own references.
	sorted, err := compute.SortTable(context.Background(), dataset.Table, keys)
	if err != nil {
		return nil, err
	}

	return &types.DataSetv2{Table: sorted}, nil
}

func (t *SortOperator) buildConfiguration(config string) (*SortConfiguration, error) {
	if len(config) < 1 {
		return nil, errors.New("invalid configuration")
	}
	// config is a json declaration of our field configuration
	typedConfig := SortConfiguration{}
	err := json.Unmarshal([]byte(config), &typedConfig)
	if err != nil {
		return nil, err
	}

	if len(typedConfig.OrderBy) < 1 {
		return nil, errors.New("missing orderby configuration")
	}

	for _, ob := range typedConfig.OrderBy {
		if len(ob.ColumnName) < 1 {
			return nil, errors.New("missing column name in orderby configuration")
		}
	}

	return &typedConfig, nil
}

func (t *SortOperator) ValidateConfiguration(config string) (bool, error) {
	typedConfig, err := t.buildConfiguration(config)
	return typedConfig != nil, err
}
