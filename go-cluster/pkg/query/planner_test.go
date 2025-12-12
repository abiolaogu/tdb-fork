package query

import (
	"context"
	"testing"
)

func TestPlanner_Aggregation(t *testing.T) {
	// Query: SELECT count(*) FROM users GROUP BY region
	stmt := &Statement{
		Select: &Select{
			Fields: []Field{
				{Aggregate: &Aggregate{Func: "COUNT", Field: "*"}},
			},
			From:    "users",
			GroupBy: []string{"region"},
		},
	}

	planner := NewPlanner(nil) // Router not needed for Aggregation plan (shards=*)
	plan, err := planner.CreatePlan(context.Background(), stmt)
	if err != nil {
		t.Fatalf("CreatePlan failed: %v", err)
	}

	if plan.Type != PlanTypeAggregation {
		t.Errorf("Expected PlanTypeAggregation, got %v", plan.Type)
	}
	if len(plan.Shards) != 1 || plan.Shards[0] != "*" {
		t.Errorf("Expected Shards=['*'], got %v", plan.Shards)
	}
}

func TestPlanner_Join(t *testing.T) {
	// Query: SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id
	stmt := &Statement{
		Select: &Select{
			Fields: []Field{
				{Name: strPtr("users.name")},
				{Name: strPtr("orders.amount")},
			},
			From: "users",
			Joins: []Join{
				{
					Type:  "INNER",
					Table: "orders",
					On:    &Condition{Left: "users.id", Operator: "=", Right: &Value{String: strPtr("orders.user_id")}}, // Simplified
				},
			},
		},
	}

	planner := NewPlanner(nil)
	plan, err := planner.CreatePlan(context.Background(), stmt)
	if err != nil {
		t.Fatalf("CreatePlan failed: %v", err)
	}

	if plan.Type != PlanTypeJoin {
		t.Errorf("Expected PlanTypeJoin, got %v", plan.Type)
	}
	if len(plan.SubPlans) != 1 {
		t.Errorf("Expected 1 SubPlan (base table scan), got %d", len(plan.SubPlans))
	}
}

func strPtr(s string) *string {
	return &s
}
