package filtrify

import (
	"github.com/liminaab/filtrify/conversion"
	"github.com/liminaab/filtrify/types"
)

func ConvertToTypedData(rawData [][]string, firstLineIsHeader bool, convertDataTypes bool, convertNumbers bool) (*types.DataSet, error) {
	return conversion.ConvertToTypedData(rawData, firstLineIsHeader, convertDataTypes, nil, convertNumbers)
}

func ConvertToTypedDatav2(rawData [][]string, firstLineIsHeader bool, convertDataTypes bool, convertNumbers bool) (*types.DataSetv2, error) {
	return conversion.ConvertToTypedDatav2(rawData, firstLineIsHeader, convertDataTypes, nil, convertNumbers)
}
