package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

// Config holds the application configuration.
type Config struct {
	ElasticsearchURL string
	IndexName        string
	QueryFile        string
	OutputFile       string
	BatchSize        int
	ScrollDuration   time.Duration
	InsecureTLS      bool
}

// Hit represents a single search hit.
type Hit struct {
	Source struct {
		Title string `json:"title"`
	} `json:"_source"`
}

func main() {
	// Parse command-line flags.
	cfg := parseFlags()

	// Create a logger.
	logger := log.New(os.Stdout, "", log.LstdFlags)

	// Set up context with cancellation for graceful shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle OS signals for graceful shutdown.
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh
		logger.Println("Received shutdown signal")
		cancel()
	}()

	// Create Elasticsearch client.
	esClient, err := createElasticsearchClient(cfg)
	if err != nil {
		logger.Fatalf("Error creating Elasticsearch client: %v", err)
	}

	// Read query from file.
	query, err := os.ReadFile(cfg.QueryFile)
	if err != nil {
		logger.Fatalf("Error reading query file: %v", err)
	}

	// Open output file.
	outputFile, err := os.Create(cfg.OutputFile)
	if err != nil {
		logger.Fatalf("Error creating output file: %v", err)
	}
	defer outputFile.Close()

	// Execute search and process results.
	if err := executeSearch(ctx, esClient, query, cfg, outputFile, logger); err != nil {
		logger.Fatalf("Error executing search: %v", err)
	}

	logger.Println("Processing completed successfully.")
}

// parseFlags parses command-line flags and returns a Config.
func parseFlags() *Config {
	cfg := &Config{}
	flag.StringVar(&cfg.ElasticsearchURL, "es-url", getEnv("ELASTICSEARCH_URL", "http://localhost:9200"), "Elasticsearch URL")
	flag.StringVar(&cfg.IndexName, "index", "sample_data", "Elasticsearch index name")
	flag.StringVar(&cfg.QueryFile, "query-file", "query.json", "Path to query JSON file")
	flag.StringVar(&cfg.OutputFile, "output-file", "data/logs.txt", "Path to output file")
	flag.IntVar(&cfg.BatchSize, "batch-size", 100, "Batch size for scroll")
	flag.DurationVar(&cfg.ScrollDuration, "scroll-duration", 1*time.Minute, "Scroll duration")
	flag.BoolVar(&cfg.InsecureTLS, "insecure-tls", false, "Skip TLS certificate verification (insecure)")
	flag.Parse()
	return cfg
}

// getEnv retrieves the value of the environment variable named by the key or returns defaultValue.
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

// createElasticsearchClient creates and returns an Elasticsearch client.
func createElasticsearchClient(cfg *Config) (*elasticsearch.Client, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: cfg.InsecureTLS,
	}
	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
	}
	esCfg := elasticsearch.Config{
		Addresses: []string{cfg.ElasticsearchURL},
		Transport: transport,
	}
	return elasticsearch.NewClient(esCfg)
}

// executeSearch performs the initial search and handles scrolling.
func executeSearch(ctx context.Context, es *elasticsearch.Client, query []byte, cfg *Config, outputFile *os.File, logger *log.Logger) error {
	var (
		scrollID  string
		totalHits int
	)

	// Perform initial search with backoff.
	res, err := retryWithBackoff(ctx, func() (*esapi.Response, error) {
		return es.Search(
			es.Search.WithContext(ctx),
			es.Search.WithIndex(cfg.IndexName),
			es.Search.WithBody(strings.NewReader(string(query))),
			es.Search.WithSize(cfg.BatchSize),
			es.Search.WithScroll(cfg.ScrollDuration),
			es.Search.WithTrackTotalHits(true),
		)
	})
	if err != nil {
		return fmt.Errorf("initial search failed: %w", err)
	}
	defer res.Body.Close()

	// Process initial response.
	result, err := parseSearchResponse(res)
	if err != nil {
		return err
	}

	scrollID = result.ScrollID
	totalHits = result.TotalHits
	logger.Printf("Total hits: %d", totalHits)

	// Process hits.
	if err := processHits(result.Hits, outputFile, logger); err != nil {
		return err
	}

	// Scroll through the rest of the results.
	for {
		if ctx.Err() != nil {
			logger.Println("Context cancelled, stopping scroll")
			break
		}

		res, err := es.Scroll(
			es.Scroll.WithContext(ctx),
			es.Scroll.WithScrollID(scrollID),
			es.Scroll.WithScroll(cfg.ScrollDuration),
		)
		if err != nil {
			return fmt.Errorf("scroll request failed: %w", err)
		}
		defer res.Body.Close()

		if res.IsError() {
			return fmt.Errorf("scroll response error: %s", res.String())
		}

		result, err := parseSearchResponse(res)
		if err != nil {
			return err
		}

		// No more hits.
		if len(result.Hits) == 0 {
			logger.Println("No more hits to process.")
			break
		}

		if err := processHits(result.Hits, outputFile, logger); err != nil {
			return err
		}

		scrollID = result.ScrollID
	}

	// Clear scroll context.
	if err := clearScroll(ctx, es, scrollID, logger); err != nil {
		logger.Printf("Error clearing scroll: %v", err)
	}

	return nil
}

// retryWithBackoff retries the given function with exponential backoff.
func retryWithBackoff(ctx context.Context, fn func() (*esapi.Response, error)) (*esapi.Response, error) {
	var res *esapi.Response
	operation := func() error {
		var err error
		res, err = fn()
		if err != nil {
			return err
		}
		if res.IsError() {
			return errors.New(res.String())
		}
		return nil
	}

	backoffCfg := backoff.NewExponentialBackOff()
	backoffCfg.InitialInterval = 1 * time.Second
	backoffCfg.MaxInterval = 30 * time.Second
	backoffCfg.MaxElapsedTime = 5 * time.Minute
	backoffCtx := backoff.WithContext(backoffCfg, ctx)

	if err := backoff.Retry(operation, backoffCtx); err != nil {
		return nil, err
	}
	return res, nil
}

// SearchResult represents the parsed search response.
type SearchResult struct {
	ScrollID  string
	TotalHits int
	Hits      []Hit
}

// parseSearchResponse parses the Elasticsearch search response.
func parseSearchResponse(res *esapi.Response) (*SearchResult, error) {
	defer res.Body.Close()

	var raw map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("error parsing response body: %w", err)
	}

	scrollID, ok := raw["_scroll_id"].(string)
	if !ok {
		return nil, errors.New("scroll ID not found in response")
	}

	totalHits, err := extractTotalHits(raw)
	if err != nil {
		return nil, err
	}

	hits, err := extractHits(raw)
	if err != nil {
		return nil, err
	}

	return &SearchResult{
		ScrollID:  scrollID,
		TotalHits: totalHits,
		Hits:      hits,
	}, nil
}

// extractTotalHits extracts the total number of hits from the response.
func extractTotalHits(raw map[string]interface{}) (int, error) {
	hitsObj, ok := raw["hits"].(map[string]interface{})
	if !ok {
		return 0, errors.New("hits not found in response")
	}
	totalObj, ok := hitsObj["total"].(map[string]interface{})
	if !ok {
		return 0, errors.New("total hits not found in response")
	}
	totalHits, ok := totalObj["value"].(float64)
	if !ok {
		return 0, errors.New("total hits value not found in response")
	}
	return int(totalHits), nil
}

// extractHits extracts the hits from the response.
func extractHits(raw map[string]interface{}) ([]Hit, error) {
	hitsObj, ok := raw["hits"].(map[string]interface{})
	if !ok {
		return nil, errors.New("hits not found in response")
	}
	hitsArray, ok := hitsObj["hits"].([]interface{})
	if !ok {
		return nil, errors.New("hits array not found in response")
	}

	var hits []Hit
	for _, h := range hitsArray {
		var hit Hit
		hitBytes, err := json.Marshal(h)
		if err != nil {
			return nil, fmt.Errorf("error marshaling hit: %w", err)
		}
		if err := json.Unmarshal(hitBytes, &hit); err != nil {
			return nil, fmt.Errorf("error unmarshaling hit: %w", err)
		}
		hits = append(hits, hit)
	}
	return hits, nil
}

// processHits processes and writes hits to the output file.
func processHits(hits []Hit, file *os.File, logger *log.Logger) error {
	for _, hit := range hits {
		if hit.Source.Title != "" {
			if _, err := file.WriteString(fmt.Sprintf("%s\n", hit.Source.Title)); err != nil {
				return fmt.Errorf("error writing to file: %w", err)
			}
		} else {
			logger.Println("Title field not found in this document.")
		}
	}
	return nil
}

// clearScroll clears the scroll context to free resources.
func clearScroll(ctx context.Context, es *elasticsearch.Client, scrollID string, logger *log.Logger) error {
	res, err := es.ClearScroll(
		es.ClearScroll.WithContext(ctx),
		es.ClearScroll.WithScrollID(scrollID),
	)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.IsError() {
		logger.Printf("Error in clear scroll response: %s", res.String())
	}
	return nil
}
