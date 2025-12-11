package query

import (
	"testing"
)

func TestParseSelect(t *testing.T) {
	sql := "SELECT id, name FROM users WHERE age > 18 LIMIT 10"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if stmt.Select == nil {
		t.Fatal("Expected Select statement")
	}

	if stmt.Select.From != "users" {
		t.Errorf("Expected FROM users, got %s", stmt.Select.From)
	}

	if len(stmt.Select.Fields) != 2 {
		t.Errorf("Expected 2 fields, got %d", len(stmt.Select.Fields))
	}

	if stmt.Select.Limit == nil || *stmt.Select.Limit != 10 {
		t.Error("Expected LIMIT 10")
	}
}
