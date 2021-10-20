package operator

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/araddon/qlbridge/lex"
	_ "github.com/araddon/qlbridge/qlbdriver"
	"github.com/araddon/qlbridge/schema"
	"limina.com/dyntransformer/lmnqlbridge"
	"limina.com/dyntransformer/types"
)

type NewColumnOperator struct {
}

type NewColumnConfiguration struct {
	Statement string `json:"statement"`
	GroupBy   string `json:"groupby"`
}

func (t *NewColumnOperator) Transform(dataset *types.DataSet, config string) (*types.DataSet, error) {

	typedConfig, err := t.buildConfiguration(config)
	if err != nil {
		return nil, err
	}
	fmt.Println(typedConfig)

	headers, _ := extractHeadersAndTypeMap(dataset)

	var sb strings.Builder
	sb.WriteString("SELECT ")
	// we can't select original columns if there is a group by statement
	if typedConfig.GroupBy == "" {
		sb.WriteString(strings.Join(headers, ","))
		sb.WriteString(", ")
	}
	sb.WriteString(typedConfig.Statement)
	sb.WriteString(" FROM ")
	sb.WriteString(defaultTableName)
	if typedConfig.GroupBy != "" {
		sb.WriteString(" GROUP BY ")
		sb.WriteString(typedConfig.GroupBy)
	}
	if err != nil {
		return nil, err
	}
	fullQuery := sb.String()

	exit := make(chan bool)
	inMemoryDataSource := lmnqlbridge.NewLmnInMemDataSource(exit)

	inMemoryDataSource.AddTable(defaultTableName, dataset)
	schemaName := RandStringBytesMaskImprSrcUnsafe(15)

	err = schema.RegisterSourceAsSchema(schemaName, inMemoryDataSource)
	if err != nil {
		return nil, err
	}
	defer func() {
		schema.DefaultRegistry().SchemaDrop(schemaName, schemaName, lex.TokenSchema)
	}()
	result, columns, err := lmnqlbridge.RunQLQuery(schemaName, fullQuery)
	if err != nil {
		return nil, err
	}
	ds := convertToDataSet(result, columns)
	return ds, nil
}

func (t *NewColumnOperator) buildConfiguration(config string) (*NewColumnConfiguration, error) {
	if len(config) < 1 {
		return nil, errors.New("invalid configuration")
	}
	// config is a json declaration of our field configuration
	typedConfig := NewColumnConfiguration{}
	err := json.Unmarshal([]byte(config), &typedConfig)
	if err != nil {
		return nil, err
	}

	if typedConfig.Statement == "" {
		return nil, errors.New("invalid configuration")
	}

	return &typedConfig, nil
}

func (t *NewColumnOperator) ValidateConfiguration(config string) (bool, error) {
	typedConfig, err := t.buildConfiguration(config)
	return typedConfig != nil, err
}
