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

func buildTestFilterSteps() []*types.TransformationStep {
	filterStep1 := &types.TransformationStep{
		Operator: types.Filter,
	}
	conf1 := operator.FilterConfiguration{
		FilterCriteria: &operator.FilterCriteria{
			Criteria: &operator.Criteria{
				FieldName: "balance",
				Operator:  ">",
				Value:     "300",
			},
		},
	}
	b1, err := json.Marshal(conf1)
	if err != nil {
		panic(err.Error())
	}
	filterStep1.Configuration = string(b1)

	filterStep2 := &types.TransformationStep{
		Operator: types.Filter,
	}
	createdAtStatement := &operator.FilterCriteria{
		Criteria: &operator.Criteria{
			FieldName: "created_at",
			Operator:  ">",
			Value:     "2008-06-02T15:04:05",
		},
	}
	activeStatement := &operator.FilterCriteria{
		Criteria: &operator.Criteria{
			FieldName: "is_active",
			Operator:  "=",
			Value:     "TRUE",
		},
	}
	conf2 := operator.FilterConfiguration{
		FilterCriteria: &operator.FilterCriteria{
			NestedCriterias: []*operator.FilterCriteria{createdAtStatement, activeStatement},
			ChainWith:       []string{"AND"},
		},
	}
	b2, err := json.Marshal(conf2)
	if err != nil {
		panic(err.Error())
	}
	filterStep2.Configuration = string(b2)

	// s1 := "ifel(balance < 300, abs(-250.23), ifel(5 > 3, round(2.5666, 3), 5)) AS wealth, " +
	// 	"floorx(4.688, 2) as flr, " +
	// 	"minx(balance, 700) as minb, " +
	// 	"maxx(balance, 700) as maxb"

	// newColStep1 := &types.TransformationStep{
	// 	Step:          3,
	// 	Enabled:       true,
	// 	Operator:      types.NewColumn,
	// 	Configuration: "{\"statement\": \"" + s1 + "\"}",
	// }

	// s2 := "average(balance) AS b, sum(balance) AS s, waverage(balance, weight) AS w, first(name) AS n, last(name) AS l, " +
	// 	"mincol(balance) AS mb, maxcol(weight) AS mw"

	// newColStep2 := &types.TransformationStep{
	// 	Step:          4,
	// 	Enabled:       true,
	// 	Operator:      types.NewColumn,
	// 	Configuration: "{\"statement\": \"" + s2 + "\", \"groupby\": \"is_active\"}",
	// }

	s3 := "left(name, 3) AS fn, right(name, 3) AS ln, split(ugly_data, ';', 2) AS sp"
	newColStep3 := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"" + s3 + "\"}",
	}

	steps := make([]*types.TransformationStep, 0)
	// steps = append(steps, filterStep2)
	// steps = append(steps, filterStep1)
	// steps = append(steps, newColStep1)
	// steps = append(steps, newColStep2)
	steps = append(steps, newColStep3)
	return steps
}

func main() {
	// let's prepare dummy operations here
	// load CSV files
	walletHeaders, wallets, err := loadCSVFileFromDataDir("wallet_csv")
	if err != nil {
		panic(err)
	}
	wallets = append([][]string{walletHeaders}, wallets...)
	ds, err := dyntransformer.ConvertToTypedData(wallets, true, true)
	if err != nil {
		panic(err.Error())
	}
	steps := buildTestFilterSteps()
	newData, err := dyntransformer.Transform(ds, steps, nil)
	if err != nil {
		panic(err.Error())
	}
	fmt.Println(newData)
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
