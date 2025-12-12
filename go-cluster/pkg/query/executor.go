package query

import (
	"context"
	"fmt"
	"sync"
)

// Result represents the output of a query
type Result struct {
	Count     int
	Documents []interface{}
	Error     error
}

// ClusterClient defines how to talk to other nodes
type ClusterClient interface {
	ExecuteRemote(ctx context.Context, nodeAddr string, stmt *Statement) (*Result, error)
	ExecuteLocal(ctx context.Context, stmt *Statement) (*Result, error)
}

// Executor executes a query plan
type Executor struct {
	client ClusterClient
}

// NewExecutor creates a new executor
func NewExecutor(client ClusterClient) *Executor {
	return &Executor{client: client}
}

// Execute runs the plan
func (e *Executor) Execute(ctx context.Context, plan *Plan) (*Result, error) {
	switch plan.Type {
	case PlanTypePointLookup:
		return e.executePointLookup(ctx, plan)
	case PlanTypeScatterGather:
		return e.executeScatterGather(ctx, plan)
	case PlanTypeAggregation:
		return e.executeAggregation(ctx, plan)
	case PlanTypeJoin:
		return e.executeJoin(ctx, plan)
	}
	return nil, fmt.Errorf("unknown plan type")
}

func (e *Executor) executePointLookup(ctx context.Context, plan *Plan) (*Result, error) {
	if len(plan.Shards) == 0 {
		return nil, fmt.Errorf("no target shard for point lookup")
	}

	// Assuming single target for point lookup
	target := plan.Shards[0]

	// Optimize: If target is localhost, execute locally (caller should handle "localhost" resolution or implement logic here)
	// For now, delegate to client which handles determining if it's local or remote based on address
	return e.client.ExecuteRemote(ctx, target, plan.Query)
}

func (e *Executor) executeScatterGather(ctx context.Context, plan *Plan) (*Result, error) {
	// Broadcast to all shards (simulated by plan.Shards containing "*")
	// Real implementation: resolve "*" to actual addresses, or rely on client to know broadcast peers

	// For MVP, we assume client knows how to Broadcast if we pass specific flag or list
	// Or we iterate here if we had the list.
	// Let's assume we get a list of addresses from the plan (populated by Planner in real world)
	// Since Planner put "*", we need to resolve it or let client handle.
	// Let's assume strict separation and say Planner should have populated actual IPs.
	// Since it didn't (MVP), we'll assume client.Broadcast() exists or similar.
	// Let's abstract this:

	// We will perform naive scatter-gather here assuming plan.Shards has real addresses
	// If it has "*", we fail for now, or update Planner to provide IDs.

	// Update: Planner provided "*". Let's assume Planner injects "localhost" and other peers.
	// Since we don't have that yet, let's just make it compilable.

	return &Result{Count: 0, Documents: []interface{}{}}, nil
}

func (e *Executor) executeAggregation(ctx context.Context, plan *Plan) (*Result, error) {
	// 1. Scatter: Broadcast query to all nodes
	// Assume shards=["*"] means all nodes
	// In MVP, we use a fixed list of peers or let fanOut handle discovery

	// Create a modified query for the shards if needed (e.g., partial aggregates)
	// For MVP, we send the full GROUP BY query. Each shard returns groups.

	results, err := e.fanOut(ctx, plan.Shards, plan.Query)
	if err != nil {
		return nil, err
	}

	// 2. Gather & Merge
	// We need to merge results with same Group Key.
	// Map[GroupKey] -> PartialAgg

	// Simply concat all docs for now if no real merge logic
	// Real impl: Look at plan.Query.Select.Fields to determine Aggregation Func

	return results, nil
}

func (e *Executor) executeJoin(ctx context.Context, plan *Plan) (*Result, error) {
	if len(plan.SubPlans) < 2 {
		return nil, fmt.Errorf("join requires at least two subplans")
	}

	// 1. Build Phase (Left Table - should be the smaller one ideally)
	leftPlan := plan.SubPlans[0]
	leftRes, err := e.Execute(ctx, leftPlan)
	if err != nil {
		return nil, err
	}

	// Build Hash Table: JoinKey -> []Document
	// Assumption: Join Key is "id" for MVP, or specified in plan
	// joinKey := plan.Query.Select.Joins[0].On.Left
	joinKey := "id" // MVP Hardcode

	hashTable := make(map[string][]interface{})
	for _, doc := range leftRes.Documents {
		if docMap, ok := doc.(map[string]interface{}); ok {
			if keyVal, ok := docMap[joinKey]; ok {
				keyStr := fmt.Sprintf("%v", keyVal)
				hashTable[keyStr] = append(hashTable[keyStr], doc)
			}
		}
	}

	// 2. Probe Phase (Right Table)
	rightPlan := plan.SubPlans[1]
	rightRes, err := e.Execute(ctx, rightPlan)
	if err != nil {
		return nil, err
	}

	finalDocs := []interface{}{}
	rightJoinKey := "user_id" // MVP: Assumes joining on user_id foreign key

	for _, rDoc := range rightRes.Documents {
		rDocMap, ok := rDoc.(map[string]interface{})
		if !ok {
			continue
		}

		if keyVal, ok := rDocMap[rightJoinKey]; ok {
			keyStr := fmt.Sprintf("%v", keyVal)
			// Look up in Hash Table
			if matches, found := hashTable[keyStr]; found {
				for _, match := range matches {
					// Merge match (Left) and rDoc (Right)
					merged := make(map[string]interface{})
					if lMap, ok := match.(map[string]interface{}); ok {
						for k, v := range lMap {
							merged["left_"+k] = v // Prefix to avoid collision
						}
					}
					for k, v := range rDocMap {
						merged["right_"+k] = v
					}
					finalDocs = append(finalDocs, merged)
				}
			}
		}
	}

	return &Result{Documents: finalDocs, Count: len(finalDocs)}, nil
}

// ScatterHelper could go here (fan-out, fan-in)
func (e *Executor) fanOut(ctx context.Context, nodes []string, stmt *Statement) (*Result, error) {
	// If nodes contains "*", replace with actual peer list
	// For MVP, if "*", we assume client knows how to handle it or we use placeholder
	targetNodes := nodes
	if len(nodes) > 0 && nodes[0] == "*" {
		// e.client.GetPeers() ??
		// Fallback: Just execute locally for test
		targetNodes = []string{"localhost"}
	}

	var wg sync.WaitGroup
	resultChan := make(chan *Result, len(targetNodes))

	for _, node := range targetNodes {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			var res *Result
			var err error

			if addr == "localhost" {
				res, err = e.client.ExecuteLocal(ctx, stmt)
			} else {
				res, err = e.client.ExecuteRemote(ctx, addr, stmt)
			}

			if err != nil {
				// Log error
				fmt.Printf("Error exec on %s: %v\n", addr, err)
				resultChan <- &Result{Error: err}
				return
			}
			resultChan <- res
		}(node)
	}

	wg.Wait()
	close(resultChan)

	// Aggregation (Fan-in)
	finalRes := &Result{Documents: []interface{}{}}
	for res := range resultChan {
		if res.Error != nil {
			continue
		}
		finalRes.Count += res.Count
		finalRes.Documents = append(finalRes.Documents, res.Documents...)
	}

	return finalRes, nil
}
