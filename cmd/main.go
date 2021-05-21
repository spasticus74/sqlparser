package main

import (
	//  "strings"
	"fmt"
	"log"

	"github.com/spasticus74/sqlparser"
)

func main() {

	str := `select start, end from there where this = that`
	q, err := sqlparser.Parse(str)

	if err != nil {
		log.Println(err)
	}
	fmt.Printf("%+#v", q)
}
