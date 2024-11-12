package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

func main() {

	// Read Elasticsearch URL from environment variable
	esURL := os.Getenv("ELASTICSEARCH_URL")
	if esURL == "" {
		esURL = "http://localhost:9200"
	}

	fmt.Printf("[DEBUG!!!] esURL: %s\n\n", esURL)
	// Configure TLS settings to skip certificate verification (use with caution)
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}

	// Create a custom HTTP transport with the TLS configuration
	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
	}

	// Configure the Elasticsearch client
	cfg := elasticsearch.Config{
		Addresses: []string{esURL},
		Transport: transport,
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	// // Define command-line flags
	// queryFile := flag.String("query-file", "", "Path to the JSON file containing the Elasticsearch query")
	// flag.Parse()

	// if *queryFile == "" {
	// 	log.Fatal("Please provide the path to the query file using the -query-file flag.")
	// }

	// // Read the query from the specified file
	// query, err := ioutil.ReadFile(*queryFile)
	// if err != nil {
	// 	log.Fatalf("Error reading query file: %s", err)
	// }

	// Read the query from query.json
	queryFile, err := os.Open("query.json")
	if err != nil {
		log.Fatalf("Error opening query.json: %s", err)
	}
	defer queryFile.Close()

	query, err := ioutil.ReadAll(queryFile)
	if err != nil {
		log.Fatalf("Error reading query.json: %s", err)
	}

	fmt.Printf("[DEBUG!!!] query 1: %s\n\n", query)
	fmt.Printf("[DEBUG!!!] query 2: %v\n\n", string(query))
	fmt.Printf("[DEBUG!!!] query 3: %v\n\n", strings.NewReader(string(query)))

	// Initialize the Elasticsearch client with the custom transport
	// es, err := elasticsearch.NewClient(elasticsearch.Config{
	// 	Addresses: []string{
	// 		"http://localhost:9200", // Replace with your Elasticsearch host
	// 	},
	// 	Transport: transport,
	// })
	// if err != nil {
	// 	log.Fatalf("Error creating the Elasticsearch client: %s", err)
	// }

	// Open the output file
	file, err := os.Create("logs.txt")
	if err != nil {
		log.Fatalf("Error creating the file: %s", err)
	}
	defer file.Close()

	// Define the batch size
	batchSize := 6

	// Define exponential backoff parameters
	backoffConfig := backoff.NewExponentialBackOff()
	backoffConfig.InitialInterval = 1 * time.Second
	backoffConfig.MaxInterval = 30 * time.Second
	backoffConfig.MaxElapsedTime = 5 * time.Minute

	// Function to execute the initial search request with scroll
	var res *esapi.Response
	err = backoff.Retry(func() error {
		var err error
		res, err = es.Search(
			es.Search.WithContext(context.Background()),
			es.Search.WithIndex("sample_data"), // Replace with your index name
			es.Search.WithBody(strings.NewReader(string(query))),
			es.Search.WithSize(batchSize),
			es.Search.WithScroll(time.Minute), // Scroll duration
			es.Search.WithTrackTotalHits(true),
			es.Search.WithPretty(),
		)
		if err != nil {
			log.Printf("Error getting response from Elasticsearch: %s", err)
			return err
		}
		if res.IsError() {
			log.Printf("Error in search response: %s", res.String())
			return fmt.Errorf("search response error")
		}
		return nil
	}, backoffConfig)
	if err != nil {
		log.Fatalf("Failed to execute initial search request: %s", err)
	}
	defer res.Body.Close()

	// Parse the initial response
	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}

	// Extract the scroll ID
	scrollID, ok := result["_scroll_id"].(string)
	if !ok {
		log.Fatalf("Error retrieving scroll ID")
	}

	// Extract the total number of hits
	totalHits := int(result["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64))
	totalPages := (totalHits + batchSize - 1) / batchSize
	log.Printf("Total hits: %d, Total pages: %d", totalHits, totalPages)

	// Process the initial batch of hits
	currentPage := 1
	log.Printf("Processing page %d of %d", currentPage, totalPages)
	processHits(result["hits"].(map[string]interface{})["hits"].([]interface{}), file)

	// Loop to retrieve the remaining batches
	for {
		currentPage++
		log.Printf("Processing page %d of %d", currentPage, totalPages)

		// Function to execute the scroll request
		err = backoff.Retry(func() error {
			var err error
			res, err = es.Scroll(
				es.Scroll.WithContext(context.Background()),
				es.Scroll.WithScrollID(scrollID),
				es.Scroll.WithScroll(time.Minute), // Scroll duration
			)
			if err != nil {
				log.Printf("Error during scroll: %s", err)
				return err
			}
			if res.IsError() {
				log.Printf("Error in scroll response: %s", res.String())
				return fmt.Errorf("scroll response error")
			}
			return nil
		}, backoffConfig)
		if err != nil {
			log.Fatalf("Failed to execute scroll request: %s", err)
		}
		defer res.Body.Close()

		// Parse the scroll response
		if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
			log.Fatalf("Error parsing the scroll response body: %s", err)
		}

		// Check if there are no more hits
		hits := result["hits"].(map[string]interface{})["hits"].([]interface{})
		if len(hits) == 0 {
			fmt.Println("No more hits to process.")
			break
		}

		// Process the current batch of hits
		log.Printf("Processing page %d of %d", currentPage, totalPages)
		processHits(hits, file)

		// Update the scroll ID
		scrollID, ok = result["_scroll_id"].(string)
		if !ok {
			log.Fatalf("Error retrieving scroll ID")
		}
	}

	// Clear the scroll context to free resources
	_, err = es.ClearScroll(
		es.ClearScroll.WithContext(context.Background()),
		es.ClearScroll.WithScrollID(scrollID),
	)
	if err != nil {
		log.Fatalf("Error clearing scroll: %s", err)
	}
}

// Function to process and write hits to the file
func processHits(hits []interface{}, file *os.File) {
	fmt.Println("Process the next batch of Hits")
	for _, hit := range hits {
		source := hit.(map[string]interface{})["_source"].(map[string]interface{})
		if message, found := source["title"]; found {
			// Write the message to the file
			if _, err := file.WriteString(fmt.Sprintf("%s\n", message)); err != nil {
				log.Fatalf("Error writing to the file: %s", err)
			}
		} else {
			log.Println("Message field not found in this document.")
		}
	}
}
