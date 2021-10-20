package operator

import (
	"math/rand"
	"time"
	"unsafe"

	"github.com/araddon/qlbridge/expr/builtins"
	"limina.com/dyntransformer/lmnqlbridge"
	"limina.com/dyntransformer/types"
)

const defaultTableName string = "ext"

func init() {
	builtins.LoadAllBuiltins()
	lmnqlbridge.LoadLiminaOperators()
}

var src = rand.NewSource(time.Now().UnixNano())

const letterBytes = "abcdefghijklmnopqrstuvwxyz123456789"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func extractHeadersAndTypeMap(dataset *types.DataSet) ([]string, map[string]types.CellDataType) {
	columnTypeMap := make(map[string]types.CellDataType)
	columnLength := len(dataset.Rows[0].Columns)
	cols := make([]string, columnLength)
	for i := 0; i < columnLength; i++ {
		cols[i] = *dataset.Rows[0].Columns[i].ColumnName
		columnTypeMap[*dataset.Rows[0].Columns[i].ColumnName] = dataset.Rows[0].Columns[i].CellValue.DataType
	}

	return cols, columnTypeMap
}

func RandStringBytesMaskImprSrcUnsafe(n int) string {
	b := make([]byte, n)
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
