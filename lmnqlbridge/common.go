package lmnqlbridge

import (
	"sync"

	"github.com/araddon/qlbridge/expr"
	"github.com/liminaab/filtrify/lmnqlbridge/operator"
)

var loadOnce sync.Once

var operators map[string]expr.CustomFunc = map[string]expr.CustomFunc{
	"average":          &operator.Average{},
	"weighted_average": &operator.WeightedAverage{},
	"first":            &operator.First{},
	"last":             &operator.Last{},
	"mincol":           &operator.MinCol{},
	"maxcol":           &operator.MaxCol{},
	"sumx":             &operator.Sum{},
	"ifel":             &operator.IF{},
	"abs":              &operator.ABS{},
	"round":            &operator.Round{},
	"floorx":           &operator.Floor{},
	"minx":             &operator.MIN{},
	"maxx":             &operator.MAX{},
	"left":             &operator.Left{},
	"right":            &operator.Right{},
	"split":            &operator.Split{},
	"concat":           &operator.Concat{},
	"containsx":        &operator.Contains{},
	"notcontainsx":     &operator.NotContains{},
	"today":            &operator.Today{},
	"time":             &operator.Time{},
	"eval":             &operator.Eval{},
	"lmnagg":           &operator.LiminaAgg{},
	"todate":           &operator.ToDate{},
	"totime":           &operator.ToTime{},
	"plus":             &operator.PLUS{},
	"minus":            &operator.MINUS{},
	"multiply":         &operator.MULTIPLY{},
	"divide":           &operator.DIVIDE{},
	"length":           &operator.Length{},
	"trim":             &operator.Trim{},
	"plusdays":         &operator.Plusdays{},
	"median":           &operator.Median{},
	"replace":          &operator.Replace{},
	"date":             &operator.Date{},
	"datetimeutc":      &operator.DateTimeUTC{},
	// we are removing it for now - qlbridge has built in and or functions
	//"and":              &operator.AND{},
	//"or":               &operator.OR{},
}

func GetOperators() map[string]expr.CustomFunc {
	return operators
}

func LoadLiminaOperators() {
	loadOnce.Do(func() {
		for key, op := range operators {
			expr.FuncAdd(key, op)
		}
	})
}

func InjectFunction(key string, op expr.CustomFunc) {
	expr.FuncAdd(key, op)
}
