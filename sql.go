package sqlparser

import (
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"

	"github.com/spasticus74/sqlparser/query"
)

// Parse takes a string representing a SQL query and parses it into a query.Query struct. It may fail.
func Parse(sqls string) (query.Query, error) {

	sqls = strings.Replace(sqls, "`", "", -1)

	space := regexp.MustCompile(`\s+`)
	sqls = space.ReplaceAllString(sqls, " ")

	qs, err := ParseMany([]string{sqls})
	if len(qs) == 0 {
		return query.Query{}, err
	}
	return qs[0], err
}

// ParseMany takes a string slice representing many SQL queries and parses them into a query.Query struct slice.
// It may fail. If it fails, it will stop at the first failure.
func ParseMany(sqls []string) ([]query.Query, error) {
	qs := []query.Query{}

	space := regexp.MustCompile(`\s+`)

	for _, sql := range sqls {
		sql = strings.Replace(sql, "`", "", -1)
		sql = space.ReplaceAllString(sql, " ")

		q, err := parse(sql)
		if err != nil {
			return qs, err
		}
		qs = append(qs, q)
	}
	return qs, nil
}

func parse(sql string) (query.Query, error) {
	return (&parser{0, strings.TrimSpace(sql), stepType, query.Query{}, nil, ""}).parse()
}

type step int

const (
	stepType step = iota
	stepTop
	stepSelectField
	stepSelectFrom
	stepSelectComma
	stepSelectFromTable
	stepInsertTable
	stepInsertFieldsOpeningParens
	stepInsertFields
	stepInsertFieldsCommaOrClosingParens
	stepInsertValuesOpeningParens
	stepInsertValuesRWord
	stepInsertValues
	stepInsertValuesCommaOrClosingParens
	stepInsertValuesCommaBeforeOpeningParens
	stepUpdateTable
	stepUpdateSet
	stepUpdateField
	stepUpdateEquals
	stepUpdateValue
	stepUpdateComma
	stepDeleteFromTable
	stepWhere
	stepWhereField
	stepWhereOperator
	stepWhereValue
	stepWhereAnd
	stepOrder
	stepOrderField
	stepOrderDirectionOrComma
	stepJoin
	stepJoinTable
	stepJoinCondition
)

type parser struct {
	i               int
	sql             string
	step            step
	query           query.Query
	err             error
	nextUpdateField string
}

func (p *parser) parse() (query.Query, error) {
	q, err := p.doParse()
	p.err = err
	if p.err == nil {
		p.err = p.validate()
	}
	//p.logError()
	return q, p.err
}

func (p *parser) doParse() (query.Query, error) {
	for {
		if p.i >= len(p.sql) {
			return p.query, p.err
		}
		switch p.step {
		case stepType:
			switch strings.ToUpper(p.peek()) {
			case "SELECT":
				p.query.Type = query.Select
				p.pop()
				look := p.peek()
				if strings.ToUpper(look) == "TOP" {
					p.step = stepTop
				} else {
					p.step = stepSelectField
				}
			case "INSERT INTO":
				p.query.Type = query.Insert
				p.pop()
				p.step = stepInsertTable
			case "UPDATE":
				p.query.Type = query.Update
				p.query.Updates = map[string]string{}
				p.pop()
				p.step = stepUpdateTable
			case "DELETE FROM":
				p.query.Type = query.Delete
				p.pop()
				p.step = stepDeleteFromTable
			default:
				return p.query, fmt.Errorf("invalid query type")
			}
		case stepTop:
			p.pop()
			m, err := strconv.Atoi(p.pop())
			if err != nil {
				log.Fatal("Unable to convert integer in TOP expression")
			}
			p.query.MaxRows = m
			p.step = stepSelectField
		case stepSelectField:
			identifier := p.peek()
			if !isIdentifierOrAsterisk(identifier) {
				return p.query, fmt.Errorf("at SELECT: expected field to SELECT")
			}
			p.query.Fields = append(p.query.Fields, identifier)
			p.pop()
			maybeFrom := p.peek()
			if strings.ToUpper(maybeFrom) == "FROM" {
				p.step = stepSelectFrom
				continue
			}
			p.step = stepSelectComma
		case stepSelectComma:
			commaRWord := p.peek()
			if commaRWord != "," {
				return p.query, fmt.Errorf("at SELECT: expected comma or FROM")
			}
			p.pop()
			p.step = stepSelectField
		case stepSelectFrom:
			fromRWord := p.peek()
			if strings.ToUpper(fromRWord) != "FROM" {
				return p.query, fmt.Errorf("at SELECT: expected FROM")
			}
			p.pop()
			p.step = stepSelectFromTable
		case stepSelectFromTable:
			tableName := p.peek()
			if len(tableName) == 0 {
				return p.query, fmt.Errorf("at SELECT: expected quoted table name")
			}

			if strings.Contains(tableName, ".") {
				parts := strings.Split(tableName, ".")
				p.query.Database = parts[0]
				tableName = parts[1]
			}

			p.query.TableName = tableName
			p.pop()
			look := p.peek()
			if strings.ToUpper(look) == "WHERE" {
				p.step = stepWhere
			} else if strings.ToUpper(look) == "ORDER BY" {
				p.step = stepOrder
			} else if strings.Contains(strings.ToUpper(look), "JOIN") {
				p.step = stepJoin
			}
		case stepInsertTable:
			tableName := p.peek()
			if len(tableName) == 0 {
				return p.query, fmt.Errorf("at INSERT INTO: expected quoted table name")
			}

			if strings.Contains(tableName, ".") {
				parts := strings.Split(tableName, ".")
				p.query.Database = parts[0]
				tableName = parts[1]
			}

			p.query.TableName = tableName
			p.pop()
			p.step = stepInsertFieldsOpeningParens
		case stepDeleteFromTable:
			tableName := p.peek()
			if len(tableName) == 0 {
				return p.query, fmt.Errorf("at DELETE FROM: expected quoted table name")
			}

			if strings.Contains(tableName, ".") {
				parts := strings.Split(tableName, ".")
				p.query.Database = parts[0]
				tableName = parts[1]
			}

			p.query.TableName = tableName
			p.pop()
			p.step = stepWhere
		case stepUpdateTable:

			tableName := p.peek()

			if len(tableName) == 0 {
				return p.query, fmt.Errorf("at UPDATE: expected quoted table name")
			}

			if strings.Contains(tableName, ".") {
				parts := strings.Split(tableName, ".")
				p.query.Database = parts[0]
				tableName = parts[1]
			}

			p.query.TableName = tableName
			p.pop()
			p.step = stepUpdateSet
		case stepUpdateSet:
			setRWord := p.peek()
			if setRWord != "SET" {
				return p.query, fmt.Errorf("at UPDATE: expected 'SET'")
			}
			p.pop()
			p.step = stepUpdateField
		case stepUpdateField:
			identifier := p.peek()

			if !isIdentifier(identifier) && isReservedWord(identifier) {
				//this case handles when a reserved word is used in the query
				return p.query, fmt.Errorf("at UPDATE: expected at least one field to update")
				//log.Println("Identifier Found")
			}
			p.nextUpdateField = identifier
			p.pop()
			p.step = stepUpdateEquals
		case stepUpdateEquals:
			equalsRWord := p.peek()
			if equalsRWord != "=" {
				return p.query, fmt.Errorf("at UPDATE: expected '='")
			}
			p.pop()
			p.step = stepUpdateValue
		case stepUpdateValue:
			quotedValue, ln := p.peekQuotedStringWithLength()
			if ln == 0 {
				quotedValue, ln = p.peekWithLength()
				if ln == 0 {
					return p.query, fmt.Errorf("at UPDATE: expected quoted value")
				}
			}
			p.query.Updates[p.nextUpdateField] = quotedValue
			p.nextUpdateField = ""
			p.pop()
			maybeWhere := p.peek()
			if strings.ToUpper(maybeWhere) == "WHERE" {
				p.step = stepWhere
				continue
			}
			p.step = stepUpdateComma
		case stepUpdateComma:
			commaRWord := p.peek()
			if commaRWord != "," {
				return p.query, fmt.Errorf("at UPDATE: expected ','")
			}
			p.pop()
			p.step = stepUpdateField
		case stepWhere:
			whereRWord := p.peek()
			if strings.ToUpper(whereRWord) != "WHERE" {
				return p.query, fmt.Errorf("expected WHERE")
			}
			p.pop()
			p.step = stepWhereField
		case stepWhereField:
			identifier := p.peek()
			if !isIdentifier(identifier) {
				return p.query, fmt.Errorf("at WHERE: expected field")
			}
			p.query.Conditions = append(p.query.Conditions, query.Condition{Operand1: identifier, Operand1IsField: true})
			p.pop()
			p.step = stepWhereOperator
		case stepWhereOperator:
			operator := p.peek()
			currentCondition := p.query.Conditions[len(p.query.Conditions)-1]
			switch operator {
			case "=":
				currentCondition.Operator = query.Eq
			case ">":
				currentCondition.Operator = query.Gt
			case ">=":
				currentCondition.Operator = query.Gte
			case "<":
				currentCondition.Operator = query.Lt
			case "<=":
				currentCondition.Operator = query.Lte
			case "!=":
				currentCondition.Operator = query.Ne
			default:
				return p.query, fmt.Errorf("at WHERE: unknown operator")
			}
			p.query.Conditions[len(p.query.Conditions)-1] = currentCondition
			p.pop()
			p.step = stepWhereValue
		case stepWhereValue:
			quotedValue, ln := p.peekQuotedStringWithLength()
			if ln == 0 {
				quotedValue, ln = p.peekWithLength()
				if ln == 0 {
					return p.query, fmt.Errorf("at WHERE: expected quoted value")
				}
			}
			currentCondition := p.query.Conditions[len(p.query.Conditions)-1]
			currentCondition.Operand2 = quotedValue
			currentCondition.Operand2IsField = false
			p.query.Conditions[len(p.query.Conditions)-1] = currentCondition
			p.pop()
			oWord := p.peek()
			if strings.ToUpper(oWord) == "ORDER BY" {
				p.pop()
				p.step = stepOrderField
			} else {
				p.step = stepWhereAnd
			}
		case stepWhereAnd:
			andRWord := p.peek()
			if strings.ToUpper(andRWord) != "AND" {
				return p.query, fmt.Errorf("expected AND")
			}
			p.pop()
			p.step = stepWhereField
		case stepOrder:
			orderRWord := p.peek()
			if strings.ToUpper(orderRWord) != "ORDER BY" {
				return p.query, fmt.Errorf("expected ORDER")
			}
			p.pop()
			p.step = stepOrderField
		case stepOrderField:
			identifier := p.peek()
			if !isIdentifier(identifier) {
				return p.query, fmt.Errorf("at ORDER BY: expected field to ORDER")
			}
			p.query.OrderFields = append(p.query.OrderFields, identifier)
			p.query.OrderDir = append(p.query.OrderDir, "ASC")
			p.pop()
			p.step = stepOrderDirectionOrComma
		case stepOrderDirectionOrComma:
			commaRWord := p.peek()
			if commaRWord == "," {
				p.pop()
			} else if commaRWord == "ASC" || commaRWord == "DESC" {
				p.pop()
				p.query.OrderDir[len(p.query.OrderDir)-1] = commaRWord
				continue
			}
			p.step = stepOrderField
		case stepJoin:
			joinType := p.peek()
			p.query.Joins = append(p.query.Joins, query.Join{Type: joinType, Table: "UNKNOWN"})
			p.pop()
			p.step = stepJoinTable
		case stepJoinTable:
			joinTable := p.peek()
			currentJoin := p.query.Joins[len(p.query.Joins)-1]
			currentJoin.Table = joinTable
			p.query.Joins[len(p.query.Joins)-1] = currentJoin
			p.pop()
			if strings.ToUpper(p.peek()) == "ON" {
				p.step = stepJoinCondition
			} else {
				p.step = stepOrder
			}
		case stepJoinCondition:
			p.pop()
			op1 := p.pop()
			op1split := strings.Split(op1, ".")
			if len(op1split) != 2 {
				return p.query, fmt.Errorf("at ON: expected <tablename>.<fieldname>")
			}
			currentCondition := query.JoinCondition{Table1: op1split[0], Operand1: op1split[1]}
			operator := p.peek()
			switch operator {
			case "=":
				currentCondition.Operator = query.Eq
			case ">":
				currentCondition.Operator = query.Gt
			case ">=":
				currentCondition.Operator = query.Gte
			case "<":
				currentCondition.Operator = query.Lt
			case "<=":
				currentCondition.Operator = query.Lte
			case "!=":
				currentCondition.Operator = query.Ne
			default:
				return p.query, fmt.Errorf("at ON: unknown operator")
			}
			p.pop()
			op2 := p.pop()
			op2split := strings.Split(op2, ".")
			if len(op2split) != 2 {
				return p.query, fmt.Errorf("at ON: expected <tablename>.<fieldname>")
			}
			currentCondition.Table2 = op2split[0]
			currentCondition.Operand2 = op2split[1]
			currentJoin := p.query.Joins[len(p.query.Joins)-1]
			currentJoin.Conditions = append(currentJoin.Conditions, currentCondition)
			p.query.Joins[len(p.query.Joins)-1] = currentJoin
			nextOp := p.peek()
			if strings.ToUpper(nextOp) == "WHERE" {
				p.step = stepWhere
			} else if strings.ToUpper(nextOp) == "ORDER BY" {
				p.step = stepOrder
			} else if strings.ToUpper(nextOp) == "AND" {
				p.step = stepJoinCondition
			} else if strings.Contains(strings.ToUpper(nextOp), "JOIN") {
				p.step = stepJoin
			}
		case stepInsertFieldsOpeningParens:
			openingParens := p.peek()
			if len(openingParens) != 1 || openingParens != "(" {
				return p.query, fmt.Errorf("at INSERT INTO: expected opening parens")
			}
			p.pop()
			p.step = stepInsertFields
		case stepInsertFields:
			identifier := p.peek()
			if !isIdentifier(identifier) && isReservedWord(identifier) {
				return p.query, fmt.Errorf("at INSERT INTO: expected at least one field to insert")
			}
			p.query.Fields = append(p.query.Fields, identifier)
			p.pop()
			p.step = stepInsertFieldsCommaOrClosingParens
		case stepInsertFieldsCommaOrClosingParens:
			commaOrClosingParens := p.peek()
			if commaOrClosingParens != "," && commaOrClosingParens != ")" {
				return p.query, fmt.Errorf("at INSERT INTO: expected comma or closing parens", commaOrClosingParens)
			}
			p.pop()
			if commaOrClosingParens == "," {
				p.step = stepInsertFields
				continue
			}
			p.step = stepInsertValuesRWord
		case stepInsertValuesRWord:
			valuesRWord := p.peek()
			if strings.ToUpper(valuesRWord) != "VALUES" {
				return p.query, fmt.Errorf("at INSERT INTO: expected 'VALUES'")
			}
			p.pop()
			p.step = stepInsertValuesOpeningParens
		case stepInsertValuesOpeningParens:
			openingParens := p.peek()
			if openingParens != "(" {
				return p.query, fmt.Errorf("at INSERT INTO: expected opening parens")
			}
			p.query.Inserts = append(p.query.Inserts, []string{})
			p.pop()
			p.step = stepInsertValues
		case stepInsertValues:
			quotedValue, ln := p.peekQuotedStringWithLength()
			if ln == 0 {
				quotedValue, ln = p.peekWithLength()
				if ln == 0 {
					return p.query, fmt.Errorf("at INSERT INTO: expected quoted value")
				}
			}
			p.query.Inserts[len(p.query.Inserts)-1] = append(p.query.Inserts[len(p.query.Inserts)-1], quotedValue)
			p.pop()
			p.step = stepInsertValuesCommaOrClosingParens
		case stepInsertValuesCommaOrClosingParens:
			commaOrClosingParens := p.peek()
			if commaOrClosingParens != "," && commaOrClosingParens != ")" {
				return p.query, fmt.Errorf("at INSERT INTO: expected comma or closing parens")
			}
			p.pop()
			if commaOrClosingParens == "," {
				p.step = stepInsertValues
				continue
			}
			currentInsertRow := p.query.Inserts[len(p.query.Inserts)-1]
			if len(currentInsertRow) < len(p.query.Fields) {
				return p.query, fmt.Errorf("at INSERT INTO: value count doesn't match field count")
			}
			p.step = stepInsertValuesCommaBeforeOpeningParens
		case stepInsertValuesCommaBeforeOpeningParens:
			commaRWord := p.peek()
			if strings.ToUpper(commaRWord) != "," && isReservedWord(commaRWord) {
				return p.query, fmt.Errorf("at INSERT INTO: expected comma")
			}
			p.pop()

			/// this catches an onduplicate key query and just finishes, that level of complexitiy is beyond the scope of this project
			if isReservedWord(commaRWord) == false {

				return p.query, nil

			} else {

				p.step = stepInsertValuesOpeningParens

			}
		}
	}
}

func (p *parser) peek() string {
	peeked, _ := p.peekWithLength()
	return peeked
}

func (p *parser) pop() string {
	peeked, len := p.peekWithLength()
	p.i += len
	p.popWhitespace()
	return peeked
}

func (p *parser) popWhitespace() {
	for ; p.i < len(p.sql) && p.sql[p.i] == ' '; p.i++ {
	}

}

var reservedWords = []string{"(", ")", ">=", "<=", "!=", ",", "=", ">", "<", "SELECT", "TOP", "INSERT INTO", "VALUES", "UPDATE", "DELETE FROM", "WHERE", "FROM", "SET", "ON DUPLICATE KEY UPDATE", "ORDER BY", "ASC", "DESC", "LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "JOIN", "ON"}

var reservedWordsOnly = []string{"SELECT", "TOP", "INSERT INTO", "VALUES", "UPDATE", "DELETE FROM", "WHERE", "FROM", "SET", "ON DUPLICATE KEY UPDATE", "ORDER BY", "ASC", "DESC", "LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "JOIN", "ON"}

func (p *parser) peekWithLength() (string, int) {
	if p.i >= len(p.sql) {
		return "", 0
	}
	for _, rWord := range reservedWords {
		token := strings.ToUpper(p.sql[p.i:min(len(p.sql), p.i+len(rWord))])
		if token == rWord {
			return token, len(token)
		}
	}
	if p.sql[p.i] == '\'' { // Quoted string
		return p.peekQuotedStringWithLength()
	}

	return p.peekIdentifierWithLength()
}

func (p *parser) peekQuotedStringWithLength() (string, int) {
	if len(p.sql) < p.i || p.sql[p.i] != '\'' {
		return "", 0
	}
	for i := p.i + 1; i < len(p.sql); i++ {
		if p.sql[i] == '\'' {
			return p.sql[p.i+1 : i], len(p.sql[p.i+1:i]) + 2 // +2 for the two quotes
		}
	}
	return "", 0
}

func (p *parser) peekIdentifierWithLength() (string, int) {
	for i := p.i; i < len(p.sql); i++ {
		if matched, _ := regexp.MatchString(`[\.\-a-zA-Z0-9_*]`, string(p.sql[i])); !matched {
			return p.sql[p.i:i], len(p.sql[p.i:i])
		}
	}
	return p.sql[p.i:], len(p.sql[p.i:])
}

func (p *parser) validate() error {
	if len(p.query.Conditions) == 0 && p.step == stepWhereField {
		return fmt.Errorf("at WHERE: empty WHERE clause")
	}
	if p.query.Type == query.UnknownType {
		return fmt.Errorf("query type cannot be empty")
	}
	if p.query.TableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}
	if len(p.query.Conditions) == 0 && (p.query.Type == query.Update || p.query.Type == query.Delete) {
		return fmt.Errorf("at WHERE: WHERE clause is mandatory for UPDATE & DELETE")
	}
	for _, c := range p.query.Conditions {
		if c.Operator == query.UnknownOperator {
			return fmt.Errorf("at WHERE: condition without operator")
		}
		if c.Operand1 == "" && c.Operand1IsField {
			return fmt.Errorf("at WHERE: condition with empty left side operand")
		}
		if c.Operand2 == "" && c.Operand2IsField {
			return fmt.Errorf("at WHERE: condition with empty right side operand")
		}
	}
	if p.query.Type == query.Insert && len(p.query.Inserts) == 0 {
		return fmt.Errorf("at INSERT INTO: need at least one row to insert")
	}
	if p.query.Type == query.Insert {
		for _, i := range p.query.Inserts {
			if len(i) != len(p.query.Fields) {
				return fmt.Errorf("at INSERT INTO: value count doesn't match field count")
			}
		}
	}
	return nil
}

func (p *parser) logError() {
	if p.err == nil {
		return
	}
	fmt.Println(p.sql)
	fmt.Println(strings.Repeat(" ", p.i) + "^")
	fmt.Println(p.err)
}

func isIdentifier(s string) bool {
	for _, rw := range reservedWords {
		if strings.ToUpper(s) == rw {
			return false
		}
	}
	matched, _ := regexp.MatchString("[a-zA-Z_][a-zA-Z_0-9]*", s)
	return matched
}

func isReservedWord(s string) bool {
	for _, rw := range reservedWordsOnly {
		if strings.ToUpper(s) == rw {
			return false
		}
	}
	matched, _ := regexp.MatchString("[a-zA-Z_][a-zA-Z_0-9]*", s)
	return matched
}

func isIdentifierOrAsterisk(s string) bool {
	return isIdentifier(s) || s == "*"
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
