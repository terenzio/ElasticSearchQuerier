// config/config.go
package config

import (
	"crypto/tls"
	"net/http"
	"os"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
)

type Config struct {
	ElasticsearchURL string
	BatchSize        int
	ScrollDuration   time.Duration
	OutputPath       string
	IndexName        string
}

func NewConfig() *Config {
	return &Config{
		ElasticsearchURL: getEnvWithDefault("ELASTICSEARCH_URL", "http://localhost:9200"),
		BatchSize:        6,
		ScrollDuration:   time.Minute,
		OutputPath:       "/app/data/logs.txt",
		IndexName:        "sample_data",
	}
}

func getEnvWithDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func NewESClient(cfg *Config) (*elasticsearch.Client, error) {
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, // Note: Use with caution in production
		},
	}

	esConfig := elasticsearch.Config{
		Addresses: []string{cfg.ElasticsearchURL},
		Transport: transport,
	}

	return elasticsearch.NewClient(esConfig)
}
