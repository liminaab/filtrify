package lmnqlbridge

import (
	"database/sql"
)

func RunQLQuery(dbName string, query string) ([][]string, []string, error) {

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

	// this is just stupid hijinx for getting pointers for unknown len columns
	readCols := make([]interface{}, len(cols))
	writeCols := make([]string, len(cols))
	for i := range writeCols {
		readCols[i] = &writeCols[i]
	}
	// TODO optimize this memory allocation processes
	dataset := make([][]string, 0)
	for rows.Next() {
		rows.Scan(readCols...)
		tmp := make([]string, len(writeCols))
		copy(tmp, writeCols)
		dataset = append(dataset, tmp)
	}
	return dataset, cols, nil
}
