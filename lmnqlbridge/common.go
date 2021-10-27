package lmnqlbridge

import (
	"sync"

	"github.com/araddon/qlbridge/expr"
	"limina.com/dyntransformer/lmnqlbridge/operator"
)

var loadOnce sync.Once

func LoadLiminaOperators() {
	loadOnce.Do(func() {
		expr.FuncAdd("average", &operator.Average{})
		expr.FuncAdd("waverage", &operator.WeightedAverage{})
		expr.FuncAdd("first", &operator.First{})
		expr.FuncAdd("last", &operator.Last{})
		expr.FuncAdd("mincol", &operator.MinCol{})
		expr.FuncAdd("maxcol", &operator.MaxCol{})
		expr.FuncAdd("sumx", &operator.Sum{})
		expr.FuncAdd("ifel", &operator.IF{})
		expr.FuncAdd("abs", &operator.ABS{})
		expr.FuncAdd("round", &operator.Round{})
		expr.FuncAdd("floorx", &operator.Floor{})
		expr.FuncAdd("minx", &operator.MIN{})
		expr.FuncAdd("maxx", &operator.MAX{})
		expr.FuncAdd("left", &operator.Left{})
		expr.FuncAdd("right", &operator.Right{})
		expr.FuncAdd("split", &operator.Split{})

		expr.FuncAdd("concat", &operator.Concat{})
		expr.FuncAdd("containsx", &operator.Contains{})
		expr.FuncAdd("notcontainsx", &operator.NotContains{})
	})
}
