package main

import (
	//  "strings"
	"fmt"
	"log"

	"github.com/spasticus74/sqlparser"
)

func main() {

	str := `SELECT start, middle, end FROM there where this = that order by start desc, end, middle asc`
	q, err := sqlparser.Parse(str)

	if err != nil {
		log.Println(err)
	}
	fmt.Printf("%+#v", q)
}
