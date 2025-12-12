package query

import (
	"context"
	"fmt"

	"github.com/lumadb/cluster/pkg/router"
)

// PlanType defines the execution strategy
type PlanType int

const (
	PlanTypePointLookup PlanType = iota
	PlanTypeScatterGather
	PlanTypeAggregation
	PlanTypeJoin
)

// Plan represents a distributed execution plan
// Plan represents a distributed execution plan
type Plan struct {
	Type     PlanType
	Shards   []string // Addresses of target shards/nodes
	Local    bool     // If true, execute locally
	Query    *Statement
	SubPlans []*Plan // For Joins or complex stages
}

// Planner creates execution plans
type Planner struct {
	router *router.Router
}

// NewPlanner creates a new planner
func NewPlanner(r *router.Router) *Planner {
	return &Planner{router: r}
}

// CreatePlan analyzes the statement and returns an execution plan
func (p *Planner) CreatePlan(ctx context.Context, stmt *Statement) (*Plan, error) {
	if stmt.Select != nil {
		return p.planSelect(ctx, stmt.Select)
	}
	if stmt.Insert != nil {
		return p.planInsert(ctx, stmt.Insert)
	}
	return nil, fmt.Errorf("unknown statement type")
}

func (p *Planner) planSelect(ctx context.Context, sel *Select) (*Plan, error) {
	// 1. Check for Joins (Highest Complexity)
	if len(sel.Joins) > 0 {
		// MVP: We only support a simple Left Deep Join for now (Planner Logic)
		// We treat the "From" as the primary and join others.
		// Real implementation would optimize order.

		// Create a plan for the primary table
		// Recursively plan the base table scan
		basePlan, err := p.planSimpleSelect(ctx, &Select{
			Fields: sel.Fields, // TODO: Split fields by table
			From:   sel.From,
			Where:  sel.Where, // TODO: Split where by table
		})
		if err != nil {
			return nil, err
		}

		// For now, we wrap it in a Join Plan
		// Real impl would need detailed Join Node logic
		return &Plan{
			Type:     PlanTypeJoin,
			Shards:   []string{"*"}, // Distributed Join usually involves all nodes
			Query:    &Statement{Select: sel},
			SubPlans: []*Plan{basePlan},
		}, nil
	}

	// 2. Check for Aggregations or Group By
	isAgg := false
	if len(sel.GroupBy) > 0 {
		isAgg = true
	}
	for _, f := range sel.Fields {
		if f.Aggregate != nil {
			isAgg = true
			break
		}
	}

	if isAgg {
		// Aggregation Plan: Scatter (collect partials) -> Gather (Merge)
		// 1. Create the detailed scatter plan (execute same query on all nodes)
		// 2. The executor will need to know to "merge" results
		return &Plan{
			Type:   PlanTypeAggregation,
			Shards: []string{"*"},
			Query:  &Statement{Select: sel},
		}, nil
	}

	// 3. Simple Select (Point Lookup or Scatter-Gather)
	return p.planSimpleSelect(ctx, sel)
}

func (p *Planner) planSimpleSelect(ctx context.Context, sel *Select) (*Plan, error) {
	// 1. Check for ID point lookup
	if sel.Where != nil && sel.Where.Condition != nil {
		cond := sel.Where.Condition
		if (cond.Left == "id" || cond.Left == "_id") && cond.Operator == "=" {
			// Point Lookup
			idVal := ""
			if cond.Right.String != nil {
				idVal = *cond.Right.String
			}
			// TODO: Handle numeric IDs if allowed

			target, err := p.router.RouteRead(ctx, sel.From, []byte(idVal))
			if err != nil {
				return nil, err
			}

			return &Plan{
				Type:   PlanTypePointLookup,
				Shards: []string{target},
				Query:  &Statement{Select: sel},
			}, nil
		}
	}

	// 2. Default: Scatter-Gather (Broadcast to all nodes)
	// TODO: Get list of all nodes from router/cluster
	return &Plan{
		Type:   PlanTypeScatterGather,
		Shards: []string{"*"}, // Wildcard for all
		Query:  &Statement{Select: sel},
	}, nil
}

func (p *Planner) planInsert(ctx context.Context, ins *Insert) (*Plan, error) {
	// Inserts are always routed by the primary key (assuming first key or specific field)
	// For MVP, assume explicit ID provided in values or auto-generated.
	// If ID is in keys/values, route to that shard.

	// Complex logic omitted for MVP: finding which value corresponds to ID
	// Return a dummy plan for now or route solely based on a known ID if possible.

	return &Plan{
		Type: PlanTypePointLookup,
		// Shard resolution would happen here based on extracted ID
		Shards: []string{"leader"},
		Query:  &Statement{Insert: ins},
	}, nil
}
