package main

import (
	"compress/gzip"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	Build, Revision string
)

var (
	user          string
	password      string
	database      string
	query         string
	output        string
	compress      bool
	withoutHeader bool
	sep           string
	nvl           string
	report        int
)

func init() {
	flag.StringVar(&user, "u", "", "user")
	flag.StringVar(&password, "p", "", "password")
	flag.StringVar(&database, "d", "", "database")
	flag.StringVar(&query, "q", "", "query")
	flag.StringVar(&output, "o", "", "output file")
	flag.BoolVar(&compress, "compress", false, "gzip compress")
	flag.BoolVar(&withoutHeader, "woh", false, "without header")
	flag.StringVar(&sep, "sep", ",", "csv seperator")
	flag.StringVar(&nvl, "nvl", "(null)", "null replace string")

	flag.IntVar(&report, "report", 1, "report count")

	flag.Parse()
}

func main() {
	if user == "" || password == "" || database == "" || query == "" || output == "" || sep == "" || report <= 0 {
		flag.Usage()
		os.Exit(-1)
	}

	start := time.Now()

	conn := fmt.Sprintf("%s:%s@/%s", user, password, database)

	if compress {
		output = output + ".gz"
	}
	fmt.Fprintln(os.Stderr, "=======================================================")
	fmt.Fprintf(os.Stderr, "mysql2csv (bld.%s, rev.%s)\n", Build, Revision)
	fmt.Fprintln(os.Stderr, "-------------------------------------------------------")
	fmt.Fprintln(os.Stderr, "connection string:", conn)
	fmt.Fprintln(os.Stderr, "output filename  :", output)
	fmt.Fprintln(os.Stderr, "compress         :", compress)
	fmt.Fprintln(os.Stderr, "query            :", query)
	fmt.Fprintln(os.Stderr, "=======================================================")

	db, err := sql.Open("mysql", conn)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	rows, err := db.Query(query)
	if err != nil {
		panic(err)
	}

	columns, err := rows.Columns()
	if err != nil {
		panic(err)
	}

	values := make([]sql.RawBytes, len(columns))

	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	csvFile, err := os.Create(output)

	var csvWriter *csv.Writer
	if compress {
		gz := gzip.NewWriter(csvFile)
		defer gz.Close()

		csvWriter = csv.NewWriter(gz)
	} else {
		csvWriter = csv.NewWriter(csvFile)
	}

	csvWriter.Comma = []rune(sep)[0]

	if !withoutHeader {
		err = csvWriter.Write(columns)
		if err != nil {
			panic(err)
		}
	}

	count := 0

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			panic(err)
		}

		var vals []string

		for _, col := range values {
			var val string
			if col == nil {
				val = nvl
			} else {
				val = string(col)
			}
			vals = append(vals, val)
		}
		err := csvWriter.Write(vals)
		if err != nil {
			panic(err)
		}
		count = count + 1
		if count%report == 0 {
			fmt.Fprintf(os.Stderr, "%d records ...\n", count)
		}
	}

	if err = rows.Err(); err != nil {
		panic(err)
	}

	csvWriter.Flush()

	elapsed := time.Since(start)
	fmt.Fprintf(os.Stderr, "%d records complete! took %s\n", count, elapsed)
}
