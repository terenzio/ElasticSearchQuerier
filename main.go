// main.go
package main

import (
	"context"
	"log"
	"os"
	"strings"

	"github.com/terenzio/ElasticSearchQuerier/client"
	"github.com/terenzio/ElasticSearchQuerier/config"
	"github.com/terenzio/ElasticSearchQuerier/processor"
)

func main() {
	cfg := config.NewConfig()

	// Initialize Elasticsearch client
	esClient, err := config.NewESClient(cfg)
	if err != nil {
		log.Fatalf("Failed to create Elasticsearch client: %v", err)
	}

	// Read query file
	query, err := os.ReadFile("query.json")
	if err != nil {
		log.Fatalf("Failed to read query file: %v", err)
	}


	// Set the title value as a variable
    titleValue := "Document 3"

	// Convert query to a string
    queryStr := string(query)
	// Replace the placeholder with the actual title value
    queryStr = strings.ReplaceAll(queryStr, "{{title}}", titleValue)

	// Create processor
	proc, err := processor.NewFileProcessor(cfg.OutputPath)
	if err != nil {
		log.Fatalf("Failed to create file processor: %v", err)
	}
	defer proc.Close()

	// Create ES scroll client
	scrollClient := client.NewESClient(esClient, cfg.ScrollDuration, cfg.BatchSize, cfg.IndexName)

	// Initialize search
	ctx := context.Background()
	// result, err := scrollClient.InitialSearch(ctx, query)
	result, err := scrollClient.InitialSearch(ctx, queryStr)
	if err != nil {
		log.Fatalf("Failed to perform initial search: %v", err)
	}

	totalPages := (result.Total + cfg.BatchSize - 1) / cfg.BatchSize
	currentPage := 1

	// Process initial batch
	log.Printf("Processing page %d of %d", currentPage, totalPages)
	if err := proc.ProcessHits(result.Hits); err != nil {
		log.Fatalf("Failed to process hits: %v", err)
	}

	// Process remaining batches
	for {
		currentPage++
		log.Printf("Processing page %d of %d", currentPage, totalPages)

		result, err = scrollClient.Scroll(ctx, result.ScrollID)
		if err != nil {
			log.Fatalf("Failed to scroll: %v", err)
		}

		if len(result.Hits) == 0 {
			log.Println("No more hits to process")
			break
		}

		if err := proc.ProcessHits(result.Hits); err != nil {
			log.Fatalf("Failed to process hits: %v", err)
		}
	}

	// Clear scroll
	if err := scrollClient.ClearScroll(ctx, result.ScrollID); err != nil {
		log.Printf("Warning: failed to clear scroll: %v", err)
	}
}
