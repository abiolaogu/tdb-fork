package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// User struct maps to DynamoDB item
type User struct {
	PK    string `dynamodbav:"pk"`
	SK    string `dynamodbav:"sk"`
	Name  string `dynamodbav:"name"`
	Email string `dynamodbav:"email"`
}

func main() {
	ctx := context.TODO()

	// 1. Configure the Client
	// LumaDB runs on localhost:8000 by default
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:   "aws",
			URL:           "http://localhost:8000/dynamodb",
			SigningRegion: "us-east-1",
		}, nil
	})

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithEndpointResolverWithOptions(customResolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("lumadb", "lumadb-secret", "")),
		config.WithRegion("us-east-1"),
	)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	client := dynamodb.NewFromConfig(cfg)
	tableName := "Users"

	fmt.Println("=== LumaDB Go Client Example ===")

	// 2. Put Item
	fmt.Println("\n1. Creating user...")
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"pk":    &types.AttributeValueMemberS{Value: "USER#GO_001"},
			"sk":    &types.AttributeValueMemberS{Value: "PROFILE"},
			"name":  &types.AttributeValueMemberS{Value: "Gopher"},
			"email": &types.AttributeValueMemberS{Value: "gopher@example.com"},
		},
	})
	if err != nil {
		// In a real app, handle ResourceNotFoundException if table doesn't exist
		log.Printf("Failed to put item: %v", err)
	} else {
		fmt.Println("✓ User created")
	}

	// 3. Get Item
	fmt.Println("\n2. Getting user...")
	out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "USER#GO_001"},
			"sk": &types.AttributeValueMemberS{Value: "PROFILE"},
		},
	})
	if err != nil {
		log.Printf("Failed to get item: %v", err)
	} else {
		if out.Item != nil {
			fmt.Printf("✓ Found user: %s\n", out.Item["name"].(*types.AttributeValueMemberS).Value)
		} else {
			fmt.Println("✗ User not found")
		}
	}

	// 4. Query
	fmt.Println("\n3. Querying...")
	qOut, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "USER#GO_001"},
		},
	})
	if err != nil {
		log.Printf("Query failed: %v", err)
	} else {
		fmt.Printf("✓ Found %d items\n", qOut.Count)
	}

	// 5. Update Item
	fmt.Println("\n4. Updating...")
	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "USER#GO_001"},
			"sk": &types.AttributeValueMemberS{Value: "PROFILE"},
		},
		UpdateExpression: aws.String("SET last_login = :t"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":t": &types.AttributeValueMemberS{Value: time.Now().Format(time.RFC3339)},
		},
	})
	if err == nil {
		fmt.Println("✓ Update successful")
	}

	fmt.Println("\nDone!")
}
