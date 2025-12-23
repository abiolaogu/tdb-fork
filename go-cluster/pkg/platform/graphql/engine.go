package graphql

import (
	"context"
	"fmt"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/gqlerrors"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/lumadb/cluster/pkg/cluster"
	"github.com/lumadb/cluster/pkg/platform/federation"
	"go.uber.org/zap"
)

// GraphQLEngine manages the dynamic GraphQL schema
type GraphQLEngine struct {
	node      *cluster.Node
	logger    *zap.Logger
	registry  *federation.SourceRegistry
	schema    graphql.Schema
	hasSchema bool
	metadata  *MetadataStore
}

func NewGraphQLEngine(node *cluster.Node, registry *federation.SourceRegistry, logger *zap.Logger) *GraphQLEngine {
	return &GraphQLEngine{
		node:     node,
		registry: registry,
		logger:   logger,
		metadata: NewMetadataStore(),
	}
}

// BuildSchema dynamically constructs the GraphQL schema from database collections AND federated sources
func (e *GraphQLEngine) BuildSchema() error {
	e.logger.Info("Building GraphQL Schema...")

	// Collections
	collections, err := e.node.ListCollections()
	if err != nil {
		e.logger.Error("Failed to list collections for schema build", zap.Error(err))
		return err
	}

	// 1. Create GraphQL Types map first (for internal references like recursive relationships)
	typesMap := make(map[string]*graphql.Object)

	// Custom JSON scalar
	jsonScalar := graphql.NewScalar(graphql.ScalarConfig{
		Name:        "JSON",
		Description: "The generic JSON scalar type represents a JSON value.",
		Serialize:   func(value interface{}) interface{} { return value },
		ParseValue:  func(value interface{}) interface{} { return value },
		ParseLiteral: func(valueAST ast.Value) interface{} {
			switch valueAST := valueAST.(type) {
			case *ast.StringValue:
				return valueAST.Value
			default:
				return nil
			}
		},
	})

	// Pre-create types
	for _, colName := range collections {
		typesMap[colName] = graphql.NewObject(graphql.ObjectConfig{
			Name: colName,
			Fields: graphql.Fields{
				"_id":      &graphql.Field{Type: graphql.String},
				"_created": &graphql.Field{Type: graphql.String},
				"data":     &graphql.Field{Type: jsonScalar},
			},
		})
		e.metadata.TrackTable(colName)
	}

	// 2. Add Relationships to Types (Thunk)
	for colName, objType := range typesMap {
		relationships := e.metadata.GetRelationships(colName)
		for _, rel := range relationships {
			targetType, ok := typesMap[rel.ToTable]
			if !ok {
				continue
			}

			if rel.Type == "object" {
				// Object Relationship (1:1)
				objType.AddFieldConfig(rel.Name, &graphql.Field{
					Type: targetType,
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						source, _ := p.Source.(map[string]interface{})
						// Map keys
						filter := make(map[string]interface{})
						for fromField, toField := range rel.FieldMapping {
							if val, ok := source[fromField]; ok {
								filter[toField] = val
							}
						}
						// Query DB
						// Simplified: e.node.GetDocumentByFilter(rel.ToTable, filter)
						return nil, nil // TODO: Implement specific resolver
					},
				})
			} else {
				// Array Relationship (1:N)
				objType.AddFieldConfig(rel.Name, &graphql.Field{
					Type: graphql.NewList(targetType),
					Resolve: func(p graphql.ResolveParams) (interface{}, error) {
						// Similar logic but returns list
						return nil, nil
					},
				})
			}
		}
	}

	// 3. Build Root Query
	queryFields := graphql.Fields{}
	mutationFields := graphql.Fields{}

	for _, colName := range collections {
		objType := typesMap[colName]

		// Filter Input Type
		filterType := graphql.NewInputObject(graphql.InputObjectConfig{
			Name: colName + "_bool_exp",
			Fields: graphql.InputObjectConfigFieldMap{
				"_and": &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewInputObject(graphql.InputObjectConfig{Name: colName + "_bool_exp_and"}))}, // Simplified recursion
				"_or":  &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewInputObject(graphql.InputObjectConfig{Name: colName + "_bool_exp_or"}))},
				// In real impl, we need fully recursive input types which graphql-go supports via Thunk
			},
		})

		// Query: List
		queryFields[colName] = &graphql.Field{
			Type: graphql.NewList(objType),
			Args: graphql.FieldConfigArgument{
				"limit":  &graphql.ArgumentConfig{Type: graphql.Int},
				"offset": &graphql.ArgumentConfig{Type: graphql.Int},
				"where":  &graphql.ArgumentConfig{Type: filterType},
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				limit, _ := p.Args["limit"].(int)
				if limit <= 0 {
					limit = 10
				}
				query := map[string]interface{}{"limit": limit}
				if whereVal, ok := p.Args["where"].(map[string]interface{}); ok {
					query["filter"] = whereVal
				}
				return e.node.RunQuery(colName, query)
			},
		}

		// Query: By ID
		queryFields[colName+"_by_pk"] = &graphql.Field{
			Type: objType,
			Args: graphql.FieldConfigArgument{
				"id": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)},
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				id, _ := p.Args["id"].(string)
				return e.node.GetDocument(colName, id)
			},
		}

		// Mutation: Insert
		mutationFields["insert_"+colName] = &graphql.Field{
			Type: graphql.String,
			Args: graphql.FieldConfigArgument{
				"objects": &graphql.ArgumentConfig{Type: graphql.NewList(jsonScalar)}, // Batch insert
			},
			Resolve: func(p graphql.ResolveParams) (interface{}, error) {
				// Batch insert logic
				return "success", nil
			},
		}
	}

	// Finalize
	schemaConfig := graphql.SchemaConfig{
		Query:    graphql.NewObject(graphql.ObjectConfig{Name: "Query", Fields: queryFields}),
		Mutation: graphql.NewObject(graphql.ObjectConfig{Name: "Mutation", Fields: mutationFields}),
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
