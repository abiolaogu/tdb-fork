package graphql

import (
	"context"
	"fmt"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/gqlerrors"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/lumadb/cluster/pkg/cluster"
	"go.uber.org/zap"
)

// GraphQLEngine manages the dynamic GraphQL schema
type GraphQLEngine struct {
	node          *cluster.Node
	logger        *zap.Logger
	schema        graphql.Schema
	hasSchema     bool
	remoteSchemas map[string]string // name -> url
}

func NewGraphQLEngine(node *cluster.Node, logger *zap.Logger) *GraphQLEngine {
	return &GraphQLEngine{
		node:          node,
		logger:        logger,
		remoteSchemas: make(map[string]string),
	}
}

// AddRemoteSchema registers an external GraphQL service
func (e *GraphQLEngine) AddRemoteSchema(name, url string) {
	e.remoteSchemas[name] = url
	e.hasSchema = false // Force rebuild
}

// BuildSchema dynamically constructs the GraphQL schema from database collections
func (e *GraphQLEngine) BuildSchema() error {
	e.logger.Info("Building GraphQL Schema...")

	// Root Query
	queryFields := graphql.Fields{
		"hello": &graphql.Field{
			Type: graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return "world", nil
			},
		},
		// Dynamic fields will be added here by iterating collections
		// For MVP, we expose a generic 'documents' query
	}

	// Root Mutation
	mutationFields := graphql.Fields{
		"noop": &graphql.Field{
			Type: graphql.String,
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				return "ok", nil
			},
		},
	}

	// Custom JSON scalar
	jsonScalar := graphql.NewScalar(graphql.ScalarConfig{
		Name:        "JSON",
		Description: "The generic JSON scalar type represents a JSON value.",
		Serialize: func(value interface{}) interface{} {
			return value
		},
		ParseValue: func(value interface{}) interface{} {
			return value
		},
		ParseLiteral: func(valueAST ast.Value) interface{} {
			switch valueAST := valueAST.(type) {
			case *ast.StringValue:
				return valueAST.Value
			default:
				return nil
			}
		},
	})

	// 1. List all collections to build schema dynamically
	collections, err := e.node.ListCollections()
	if err != nil {
		e.logger.Error("Failed to list collections for schema build", zap.Error(err))
		// Fallback to empty schema or simple hello world
	}

	for _, colName := range collections {
		// Define Type for Collection
		// Since schema is dynamic/schemaless, we use a generic structure with JSON data
		objType := graphql.NewObject(graphql.ObjectConfig{
			Name: colName,
			Fields: graphql.Fields{
				"_id":      &graphql.Field{Type: graphql.String},
				"_created": &graphql.Field{Type: graphql.String},
				"data":     &graphql.Field{Type: jsonScalar},
			},
		})

		// --- QUERIES ---

		// 1. Get by ID
		queryFields[colName+"_by_pk"] = &graphql.Field{
			Type: objType,
			Args: graphql.FieldConfigArgument{
				"id": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)},
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				id, _ := p.Args["id"].(string)
				// Call DB Get
				// We need access to the underlying DB instance from Node
				// Assuming e.node has a method to get specific collection data helper
				// For now, we use a direct DB call if exposed, or add helper to Node
				return e.node.GetDocument(colName, id)
			},
		}

		// 2. List (with simple filter support)
		queryFields[colName] = &graphql.Field{
			Type: graphql.NewList(objType),
			Args: graphql.FieldConfigArgument{
				"limit": &graphql.ArgumentConfig{Type: graphql.Int},
				"where": &graphql.ArgumentConfig{Type: jsonScalar},
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				limit, _ := p.Args["limit"].(int)
				if limit <= 0 {
					limit = 10
				}

				query := map[string]interface{}{}
				if limit > 0 {
					query["limit"] = limit
				}

				// Handle 'where' filter
				if whereVal, ok := p.Args["where"].(map[string]interface{}); ok {
					query["filter"] = whereVal
				}

				return e.node.RunQuery(colName, query)
			},
		}

		// --- MUTATIONS ---

		// 3. Insert
		mutationFields["insert_"+colName] = &graphql.Field{
			Type: graphql.String, // Returns ID
			Args: graphql.FieldConfigArgument{
				"data": &graphql.ArgumentConfig{Type: graphql.NewNonNull(jsonScalar)},
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				data, _ := p.Args["data"].(map[string]interface{})
				return e.node.InsertDocument(colName, data)
			},
		}

		// 4. Update
		mutationFields["update_"+colName] = &graphql.Field{
			Type: graphql.String, // Returns status/ID
			Args: graphql.FieldConfigArgument{
				"id":   &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)},
				"data": &graphql.ArgumentConfig{Type: graphql.NewNonNull(jsonScalar)},
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				id, _ := p.Args["id"].(string)
				data, _ := p.Args["data"].(map[string]interface{})
				return "ok", e.node.UpdateDocument(colName, id, data)
			},
		}

		// 5. Delete
		mutationFields["delete_"+colName] = &graphql.Field{
			Type: graphql.String, // Returns status
			Args: graphql.FieldConfigArgument{
				"id": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)},
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				id, _ := p.Args["id"].(string)
				return "ok", e.node.DeleteDocument(colName, id)
			},
		}
	}

	rootQuery := graphql.ObjectConfig{Name: "Query", Fields: queryFields}
	rootMutation := graphql.ObjectConfig{Name: "Mutation", Fields: mutationFields}

	schemaConfig := graphql.SchemaConfig{
		Query:    graphql.NewObject(rootQuery),
		Mutation: graphql.NewObject(rootMutation),
		Subscription: graphql.NewObject(graphql.ObjectConfig{
			Name: "Subscription",
			Fields: graphql.Fields{
				"noop": &graphql.Field{Type: graphql.String}, // Placeholder
			},
		}),
	}

	schema, err := graphql.NewSchema(schemaConfig)
	if err != nil {
		return fmt.Errorf("failed to create schema: %v", err)
	}

	e.schema = schema
	e.hasSchema = true
	return nil
}

// Execute runs a GraphQL query
func (e *GraphQLEngine) Execute(ctx context.Context, query string, variables map[string]interface{}) *graphql.Result {
	if !e.hasSchema {
		// Try to build schema lazily
		if err := e.BuildSchema(); err != nil {
			return &graphql.Result{Errors: []gqlerrors.FormattedError{{Message: err.Error()}}}
		}
	}

	params := graphql.Params{
		Schema:         e.schema,
		RequestString:  query,
		VariableValues: variables,
		Context:        ctx,
	}

	return graphql.Do(params)
}
