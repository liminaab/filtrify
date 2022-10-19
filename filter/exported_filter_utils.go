package filter

import (
	"github.com/liminaab/filtrify/operator"
	"github.com/liminaab/filtrify/types"
)

// Some helpers to make it easier to use the library from the outside in code

type FilterCriteria = operator.FilterCriteria

func Filter(dataset *types.DataSet, filterCriteria *FilterCriteria) (*types.DataSet, error) {
	op := operator.FilterOperator{}
	return op.TransformTyped(dataset, &operator.FilterConfiguration{FilterCriteria: filterCriteria})
}

func Or(criteria ...*FilterCriteria) *FilterCriteria {
	return &FilterCriteria{
		ChainWith:       []string{"OR"},
		NestedCriterias: criteria,
	}
}

func And(criteria ...*FilterCriteria) *FilterCriteria {
	return &FilterCriteria{
		ChainWith:       []string{"AND"},
		NestedCriterias: criteria,
	}
}

func Eq(field string, value string) *FilterCriteria {
	return &FilterCriteria{
		Criteria: &operator.Criteria{
			FieldName: field,
			Operator:  "=",
			Value:     value,
		},
	}
}

func Neq(field string, value string) *FilterCriteria {
	return &FilterCriteria{
		Criteria: &operator.Criteria{
			FieldName: field,
			Operator:  "!=",
			Value:     value,
		},
	}
}

func Gt(field string, value string) *FilterCriteria {
	return &FilterCriteria{
		Criteria: &operator.Criteria{
			FieldName: field,
			Operator:  ">",
			Value:     value,
		},
	}
}

func Gte(field string, value string) *FilterCriteria {
	return &FilterCriteria{
		Criteria: &operator.Criteria{
			FieldName: field,
			Operator:  ">=",
			Value:     value,
		},
	}
}

func Lt(field string, value string) *FilterCriteria {
	return &FilterCriteria{
		Criteria: &operator.Criteria{
			FieldName: field,
			Operator:  "<",
			Value:     value,
		},
	}
}

func Lte(field string, value string) *FilterCriteria {
	return &FilterCriteria{
		Criteria: &operator.Criteria{
			FieldName: field,
			Operator:  "<=",
			Value:     value,
		},
	}
}
