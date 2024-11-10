package main

import (
	"os"
	"testing"
)

func TestProcessHits(t *testing.T) {
	// Mock data simulating Elasticsearch hits
	hits := []interface{}{
		map[string]interface{}{
			"_source": map[string]interface{}{
				"title": "Test message 1",
			},
		},
		map[string]interface{}{
			"_source": map[string]interface{}{
				"title": "Test message 2",
			},
		},
	}

	// Create a temporary file to write the output
	tmpFile, err := os.CreateTemp("", "test_logs_*.txt")
	if err != nil {
		t.Fatalf("Error creating temporary file: %s", err)
	}
	defer os.Remove(tmpFile.Name())

	// Call the function to test
	processHits(hits, tmpFile)

	// Read the content of the file
	content, err := os.ReadFile(tmpFile.Name())
	if err != nil {
		t.Fatalf("Error reading temporary file: %s", err)
	}

	// Expected output
	expected := "Test message 1\nTest message 2\n"

	// Compare the content with the expected output
	if string(content) != expected {
		t.Errorf("Expected %q but got %q", expected, string(content))
	}
}
