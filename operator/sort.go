package operator

import (
	"encoding/json"
	"errors"
	"sort"
	"strings"

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

func (t *SortOperator) GetColumn(r *types.DataRow, col string) *types.DataColumn {
	for _, c := range r.Columns {
		if c.ColumnName == col {
			return c
		}
	}

	return nil
}

func (t *SortOperator) CompareColumns(col1 *types.DataColumn, col2 *types.DataColumn) (int, error) {
	if (col1 == nil || col1.CellValue.DataType == types.NilType) && (col2 == nil || col2.CellValue.DataType == types.NilType) {
		return 0, nil
	}

	if col1 == nil || col1.CellValue.DataType == types.NilType {
		return -1, nil
	}

	if col2 == nil || col2.CellValue.DataType == types.NilType {
		return 1, nil
	}

	if col1.CellValue.DataType != col2.CellValue.DataType {
		return 0, errors.New("invalid comparison between unrelated columns")
	}
	cell1 := col1.CellValue
	cell2 := col2.CellValue

	result := 0
	switch cell1.DataType {
	case types.IntType:
		if cell1.IntValue > cell2.IntValue {
			result = 1
		} else if cell1.IntValue < cell2.IntValue {
			result = -1
		}
	case types.LongType:
		if cell1.LongValue > cell2.LongValue {
			result = 1
		} else if cell1.LongValue < cell2.LongValue {
			result = -1
		}
	case types.TimestampType:
	case types.DateType:
	case types.TimeOfDayType:
		if cell1.TimestampValue.After(cell2.TimestampValue) {
			result = 1
		} else if cell1.TimestampValue.Before(cell2.TimestampValue) {
			result = -1
		}
	case types.StringType:
		result = strings.Compare(cell1.StringValue, cell2.StringValue)
	case types.DoubleType:
		if cell1.DoubleValue > cell2.DoubleValue {
			result = 1
		} else if cell1.DoubleValue < cell2.DoubleValue {
			result = -1
		}
	case types.BoolType:
		if cell1.BoolValue && !cell2.BoolValue {
			result = 1
		} else if !cell1.BoolValue && cell2.BoolValue {
			result = -1
		}
	}

	return result, nil

}

func (t *SortOperator) Transform(dataset *types.DataSet, config string, _ map[string]*types.DataSet) (*types.DataSet, error) {

	typedConfig, confError := t.buildConfiguration(config)
	if confError != nil {
		return nil, confError
	}
	var err error = nil
	sort.SliceStable(dataset.Rows, func(i, j int) bool {
		if err != nil {
			return true
		}
		for _, c := range typedConfig.OrderBy {
			col1 := t.GetColumn(dataset.Rows[i], c.ColumnName)
			if col1 == nil {
				err = buildColumnNotExistsError(c.ColumnName)
				return true
			}
			col2 := t.GetColumn(dataset.Rows[j], c.ColumnName)
			if col2 == nil {
				err = buildColumnNotExistsError(c.ColumnName)
				return true
			}
			result, internalErr := t.CompareColumns(col1, col2)
			if internalErr != nil {
				err = internalErr
				return true
			}
			switch result {
			case -1:
				return c.Ascending
			case 1:
				return !c.Ascending
			}
			// we need to move to next column
		}
		return true
	})

	return dataset, nil
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
