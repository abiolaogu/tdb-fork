package query

import (
	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

// AST Definitions

type Statement struct {
	Select *Select `@@`
	Insert *Insert `| @@`
	Update *Update `| @@`
	Delete *Delete `| @@`
}

type Select struct {
	Fields  []Field  ` "SELECT" @@ { "," @@ }`
	From    string   ` "FROM" @Ident`
	Joins   []Join   ` { @@ }`
	Where   *Where   ` [ "WHERE" @@ ]`
	GroupBy []string ` [ "GROUP" "BY" @Ident { "," @Ident } ]`
	OrderBy []Order  ` [ "ORDER" "BY" @@ { "," @@ } ]`
	Limit   *int     ` [ "LIMIT" @Number ]`
}

type Join struct {
	Type  string     ` ( @( "LEFT" | "RIGHT" | "INNER" | "OUTER" ) )? "JOIN"`
	Table string     ` @Ident`
	On    *Condition ` "ON" @@`
}

type Order struct {
	Field string `@Ident`
	Desc  bool   `[ @( "DESC" | "ASC" ) ]`
}

type Update struct {
	Collection string ` "UPDATE" @Ident`
	Sets       []Set  ` "SET" @@ { "," @@ }`
	Where      *Where ` [ "WHERE" @@ ]`
}

type Set struct {
	Field string `@Ident`
	Value *Value ` "=" @@`
}

type Delete struct {
	Collection string ` "DELETE" "FROM" @Ident`
	Where      *Where ` [ "WHERE" @@ ]`
}

type Insert struct {
	Collection string   ` "INSERT" "INTO" @Ident`
	Keys       []string ` "(" @Ident { "," @Ident } ")"`
	Values     []Value  ` "VALUES" "(" @@ { "," @@ } ")"`
}

type Field struct {
	Name      *string    ` ( @Ident`
	Aggregate *Aggregate ` | @@ )`
	Alias     string     ` [ "AS" @Ident ]`
}

type Aggregate struct {
	Func  string `@( "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" )`
	Field string ` "(" ( @Ident | "*" ) ")"`
}

type Where struct {
	Condition *Condition `@@`
}

type Condition struct {
	Left     string `@Ident`
	Operator string `@( "=" | "<>" | "<" | ">" | "<=" | ">=" )`
	Right    *Value `@@`
}

type Value struct {
	Number *float64 `@Number`
	String *string  `| @String`
	Bool   *bool    `| ( "TRUE" | "FALSE" )`
}

// Parser Instance
var (
	lqlLexer = lexer.MustSimple([]lexer.SimpleRule{
		{"Ident", `[a-zA-Z_]\w*`},
		{"Number", `[-+]?\d*\.?\d+`},
		{"String", `'[^']*'|"[^"]*"`},
		{"Punct", `[-[!@#$%^&*()+_={}\|:;"<,>.?/]|]`},
		{"Whitespace", `\s+`},
	})

	parser = participle.MustBuild[Statement](
		participle.Lexer(lqlLexer),
		participle.Unquote("String"),
		participle.CaseInsensitive("Ident"),
		participle.Elide("Whitespace"),
	)
)

// Parse parses an LQL string
func Parse(query string) (*Statement, error) {
	return parser.ParseString("", query)
}
