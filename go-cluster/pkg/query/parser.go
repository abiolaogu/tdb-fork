package query

import (
	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

// AST Definitions

type Statement struct {
	Select *Select `@@`
	Insert *Insert `| @@`
	// TODO: Update, Delete
}

type Select struct {
	Fields []Field ` "SELECT" @@ { "," @@ }`
	From   string  ` "FROM" @Ident`
	Where  *Where  ` [ "WHERE" @@ ]`
	Limit  *int    ` [ "LIMIT" @Number ]`
}

type Insert struct {
	Collection string   ` "INSERT" "INTO" @Ident`
	Keys       []string ` "(" @Ident { "," @Ident } ")"`
	Values     []Value  ` "VALUES" "(" @@ { "," @@ } ")"`
}

type Field struct {
	Name string `@Ident`
	// TODO: Aggregations, Aliases
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
