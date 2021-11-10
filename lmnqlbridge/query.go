package lmnqlbridge

import (
	"database/sql"
	"reflect"
)

func RunQLQuery(dbName string, query string) ([][]interface{}, []string, error) {

	db, err := sql.Open("qlbridge", dbName)
	if err != nil {
		return nil, nil, err
	}
	defer db.Close()

	rows, err := db.Query(query)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	cols, _ := rows.Columns()

	// TODO optimize this memory allocation processes
	dataset := make([][]interface{}, 0, 1000)

	for rows.Next() {
		columnTypes, err := rows.ColumnTypes()
		if err != nil {
			return nil, nil, err
		}
		values := make([]interface{}, len(cols))
		object := make([]interface{}, len(cols))
		for i, column := range columnTypes {
			// TODO column.ScanType returns Long for int values - DEBUG this
			object[i] = reflect.New(column.ScanType()).Interface()
			values[i] = object[i]
		}

		rows.Scan(values...)
		dataset = append(dataset, object)
	}
	return dataset, cols, nil
}
