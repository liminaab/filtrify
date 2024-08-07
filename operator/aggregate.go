package operator

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/araddon/qlbridge/expr"
	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/liminaab/filtrify/lmnqlbridge"
	"github.com/liminaab/filtrify/types"
)

type AggregateOperator struct {
}

type AggregateSelect struct {
	Columns []string `json:"columns"`
	Method  string   `json:"method"`
}

type AggregateConfiguration struct {
	Select  []*AggregateSelect `json:"select"`
	GroupBy []string           `json:"groupby"`
}

func (t *AggregateOperator) hasAggCall(statement string) bool {
	statement = strings.ToLower(statement)
	allOps := lmnqlbridge.GetOperators()
	for key, op := range allOps {
		aggfn, hasAggFlag := op.(expr.AggFunc)
		if hasAggFlag {
			if aggfn.IsAgg() {
				fncCallKey := key + "("
				// let's make sure our statement contains a call to an aggregate function
				if strings.Contains(statement, fncCallKey) {
					return true
				}
			}
		}
	}
	return false
}

func buildLiminaAggSelectStatement(selectAgg *AggregateSelect, index int) (string, error) {
	var sb strings.Builder
	method := strings.ToLower(selectAgg.Method)
	allOps := lmnqlbridge.GetOperators()
	for key, op := range allOps {
		aggfn, hasAggFlag := op.(expr.AggFunc)
		if hasAggFlag {
			if aggfn.IsAgg() {
				if key == method {
					sb.WriteString(key)
					sb.WriteString("(")
					for i, col := range selectAgg.Columns {
						sb.WriteString(fmt.Sprintf("`%s`", col))
						if i != len(selectAgg.Columns)-1 {
							sb.WriteString(",")
						}
					}
					sb.WriteString(fmt.Sprintf(") AS `%s`", selectAgg.Columns[0]))
					if index > 0 {
						sb.WriteString(strconv.Itoa(index))
					}
				}
			}
		}
	}

	return sb.String(), nil
}

func (t *AggregateOperator) TransformWithConfig(dataset *types.DataSet, typedConfig *AggregateConfiguration, _ map[string]*types.DataSet) (*types.DataSet, error) {
	headers, columnTypeMap := extractHeadersAndTypeMap(dataset)
	headerSelectMap := make(map[string][]*AggregateSelect)
	for _, h := range headers {
		headerSelectMap[h] = nil
	}
	for _, sel := range typedConfig.Select {
		if len(sel.Columns) < 1 {
			return nil, errors.New("invalid configuration")
		}
		colToRemoveFromUsualSelect := sel.Columns[0]
		if _, ok := headerSelectMap[colToRemoveFromUsualSelect]; !ok {
			headerSelectMap[colToRemoveFromUsualSelect] = make([]*AggregateSelect, 0)
		}
		headerSelectMap[colToRemoveFromUsualSelect] = append(headerSelectMap[colToRemoveFromUsualSelect], sel)
	}
	var sb strings.Builder
	sb.WriteString("SELECT ")
	// we need to remove this column from header if this is already being selected
	for _, h := range headers {
		v, ok := headerSelectMap[h]
		if ok && len(v) > 0 {
			for i, aggSel := range v {
				selQ, err := buildLiminaAggSelectStatement(aggSel, i)
				if err != nil {
					return nil, err
				}
				sb.WriteString(selQ)
				sb.WriteString(",")
			}
		} else {
			continue
		}
	}
	str := sb.String()
	if str[len(str)-1:] == "," {
		str = str[:len(str)-1]
	}
	sb.Reset()
	sb.WriteString(str)

	for _, gb := range typedConfig.GroupBy {
		// let's check if this column exists to group by
		_, exists := columnTypeMap[gb]
		if !exists {
			return nil, buildColumnNotExistsError(gb)
		}
		if len(typedConfig.Select) > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(fmt.Sprintf("lmnagg(`%s`) AS `%s`", gb, gb))
	}

	sb.WriteString(" FROM ")
	sb.WriteString(defaultTableName)
	if len(typedConfig.GroupBy) > 0 {
		sb.WriteString(" GROUP BY ")
		for i, gb := range typedConfig.GroupBy {
			// let's check if this column exists to group by
			_, exists := columnTypeMap[gb]
			if !exists {
				return nil, buildColumnNotExistsError(gb)
			}
			sb.WriteString(fmt.Sprintf("`%s`", gb))
			if i != len(typedConfig.GroupBy)-1 {
				sb.WriteString(",")
			}
		}
	}
	fullQuery := sb.String()
	result, err := executeSQLQuery(fullQuery, dataset, columnTypeMap)
	if err != nil {
		return nil, err
	}
	result.Headers = buildHeaders(result, dataset)
	return result, nil
}

func (t *AggregateOperator) Transform(dataset *types.DataSet, config string, _ map[string]*types.DataSet) (*types.DataSet, error) {
	typedConfig, err := t.buildConfiguration(config)
	if err != nil {
		return nil, err
	}
	return t.TransformWithConfig(dataset, typedConfig, nil)
}

func (t *AggregateOperator) buildConfiguration(config string) (*AggregateConfiguration, error) {
	if len(config) < 1 {
		return nil, errors.New("invalid configuration")
	}
	// config is a json declaration of our field configuration
	typedConfig := AggregateConfiguration{}
	err := json.Unmarshal([]byte(config), &typedConfig)
	if err != nil {
		return nil, err
	}

	if len(typedConfig.GroupBy) < 1 {
		return nil, errors.New("missing groupby in aggregate configuration")
	}

	return &typedConfig, nil
}

func (t *AggregateOperator) ValidateConfiguration(config string) (bool, error) {
	typedConfig, err := t.buildConfiguration(config)
	return typedConfig != nil, err
}
