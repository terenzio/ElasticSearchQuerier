// client/elasticsearch.go
package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type ESClient struct {
	client         *elasticsearch.Client
	scrollDuration time.Duration
	batchSize      int
	indexName      string
}

func NewESClient(client *elasticsearch.Client, scrollDuration time.Duration, batchSize int, indexName string) *ESClient {
	return &ESClient{
		client:         client,
		scrollDuration: scrollDuration,
		batchSize:      batchSize,
		indexName:      indexName,
	}
}

type ScrollResult struct {
	ScrollID string
	Hits     []map[string]interface{}
	Total    int
}

// func (c *ESClient) InitialSearch(ctx context.Context, query []byte) (*ScrollResult, error) {
func (c *ESClient) InitialSearch(ctx context.Context, query string) (*ScrollResult, error) {
	backoffConfig := newBackoffConfig()

	var res *esapi.Response
	err := backoff.Retry(func() error {
		var err error
		res, err = c.client.Search(
			c.client.Search.WithContext(ctx),
			c.client.Search.WithIndex(c.indexName),
			// c.client.Search.WithBody(strings.NewReader(string(query))),
			c.client.Search.WithBody(strings.NewReader(query)),
			c.client.Search.WithSize(c.batchSize),
			c.client.Search.WithScroll(c.scrollDuration),
			c.client.Search.WithTrackTotalHits(true),
		)
		return handleESResponse(res, err)
	}, backoffConfig)

	if err != nil {
		return nil, fmt.Errorf("initial search failed: %w", err)
	}
	defer res.Body.Close()

	return parseScrollResponse(res.Body)
}

func (c *ESClient) Scroll(ctx context.Context, scrollID string) (*ScrollResult, error) {
	backoffConfig := newBackoffConfig()

	var res *esapi.Response
	err := backoff.Retry(func() error {
		var err error
		res, err = c.client.Scroll(
			c.client.Scroll.WithContext(ctx),
			c.client.Scroll.WithScrollID(scrollID),
			c.client.Scroll.WithScroll(c.scrollDuration),
		)
		return handleESResponse(res, err)
	}, backoffConfig)

	if err != nil {
		return nil, fmt.Errorf("scroll request failed: %w", err)
	}
	defer res.Body.Close()

	return parseScrollResponse(res.Body)
}

func (c *ESClient) ClearScroll(ctx context.Context, scrollID string) error {
	_, err := c.client.ClearScroll(
		c.client.ClearScroll.WithContext(ctx),
		c.client.ClearScroll.WithScrollID(scrollID),
	)
	return err
}

func newBackoffConfig() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = 1 * time.Second
	b.MaxInterval = 30 * time.Second
	b.MaxElapsedTime = 5 * time.Minute
	return b
}

func handleESResponse(res *esapi.Response, err error) error {
	if err != nil {
		return fmt.Errorf("elasticsearch request failed: %w", err)
	}
	if res.IsError() {
		return fmt.Errorf("elasticsearch response error: %s", res.String())
	}
	return nil
}

func parseScrollResponse(body io.Reader) (*ScrollResult, error) {
	var result map[string]interface{}
	if err := json.NewDecoder(body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	scrollID, ok := result["_scroll_id"].(string)
	if !ok {
		return nil, fmt.Errorf("scroll ID not found in response")
	}

	hitsObj := result["hits"].(map[string]interface{})
	total := int(hitsObj["total"].(map[string]interface{})["value"].(float64))
	hits := hitsObj["hits"].([]interface{})

	processedHits := make([]map[string]interface{}, len(hits))
	for i, hit := range hits {
		hitMap := hit.(map[string]interface{})
		processedHits[i] = hitMap["_source"].(map[string]interface{})
	}

	return &ScrollResult{
		ScrollID: scrollID,
		Hits:     processedHits,
		Total:    total,
	}, nil
}
