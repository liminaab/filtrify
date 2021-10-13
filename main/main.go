package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"limina.com/dyntransformer"
	"limina.com/dyntransformer/operator"
	"limina.com/dyntransformer/types"
)

func main() {
	// let's prepare dummy operations here
	// load CSV files
	walletHeaders, wallets, err := loadCSVFileFromDataDir("wallet_csv")
	if err != nil {
		panic(err)
	}
	wallets = append([][]string{walletHeaders}, wallets...)
	input := &types.InputData{
		RawData:                  wallets,
		RawDataFirstLineIsHeader: true,
	}

	ds, err := dyntransformer.ConvertToTypedData(wallets, true, true)
	if err != nil {
		panic(err.Error())
	}
	fmt.Println(ds)

	// we will have a single step of filter

	filterStep := &types.TransformationStep{
		Step:          0,
		Enabled:       true,
		Operator:      types.Filter,
		Configuration: "json string",
	}
	// type TransformationStep struct {
	// 	Step          int
	// 	Enabled       bool
	// 	Operator      TransformationOperatorType
	// 	Configuration string
	// }
	steps := make([]*types.TransformationStep, 0)
	steps = append(steps, filterStep)
	conf := operator.FilterConfiguration{
		Statement: &operator.FilterStatement{
			Criteria: &operator.Criteria{
				FieldName: "balance",
				Operator:  ">",
				Value:     "300",
			},
		},
	}
	b, err := json.Marshal(conf)
	if err != nil {
		panic(err.Error())
	}
	filterStep.Configuration = string(b)
	dyntransformer.Transform(input, steps)

	dyntransformer.Transform(input, steps)
}

func loadCSVFile(filePath string) (headers []string, dataset [][]string, err error) {
	var ior io.Reader
	ior, err = os.Open(filePath)
	if err != nil {
		return nil, nil, err
	}
	// let's read columns
	rc, ok := ior.(io.ReadCloser)
	defer rc.Close()
	if !ok {
		return nil, nil, errors.New("File error possibly huh?")
	}

	buf := bufio.NewReader(ior)
	csvr := csv.NewReader(buf)
	csvr.TrailingComma = true
	headers, err = csvr.Read()
	if err != nil {
		return nil, nil, err
	}
	// now lets load the complete file into memory
	dataset = make([][]string, 0, 10000)
	for {
		var row []string
		row, err = csvr.Read()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return headers, dataset, err
		}
		dataset = append(dataset, row)
	}

}

func loadCSVFileFromDataDir(fileName string) (headers []string, dataset [][]string, err error) {
	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	exPath := filepath.Dir(ex)
	exPath = path.Join(exPath, "..")
	fmt.Println(exPath)
	dataPath := path.Join(exPath, "testdata")
	fullPath := filepath.Join(dataPath, fileName)
	return loadCSVFile(fullPath)
}
