package lmnqlbridge

import (
	"sync"

	"github.com/araddon/qlbridge/expr"
	"github.com/araddon/qlbridge/expr/builtins"
	"limina.com/dyntransformer/lmnqlbridge/operator"
)

var loadOnce sync.Once

func LoadLiminaOperators() {
	loadOnce.Do(func() {
		expr.FuncAdd("average", &builtins.Avg{})
		expr.FuncAdd("averagexx", &operator.Average{})
		expr.FuncAdd("ifel", &operator.IF{})
		expr.FuncAdd("abs", &operator.ABS{})
		expr.FuncAdd("round", &operator.Round{})
		expr.FuncAdd("floorx", &operator.Floor{})
		expr.FuncAdd("minx", &operator.MIN{})
		expr.FuncAdd("maxx", &operator.MAX{})
	})
}
