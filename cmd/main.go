package main


import (
  "log"
  "github.com/ralfonso-directnic/sqlparser"
)


func main(){ 

  str :=""
  
  q,err := sqlparser.Parse(str)

  if err != nil {
        log.Println(err)
  }
  log.Println(q)
}
