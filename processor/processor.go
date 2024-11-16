// processor/processor.go
package processor

import (
	"fmt"
	"os"
)

type FileProcessor struct {
	file *os.File
}

func NewFileProcessor(filepath string) (*FileProcessor, error) {
	file, err := os.Create(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to create output file: %w", err)
	}
	return &FileProcessor{file: file}, nil
}

func (p *FileProcessor) ProcessHits(hits []map[string]interface{}) error {
	for _, hit := range hits {
		if message, ok := hit["title"]; ok {
			if _, err := p.file.WriteString(fmt.Sprintf("%s\n", message)); err != nil {
				return fmt.Errorf("failed to write to file: %w", err)
			}
		}
	}
	return nil
}

func (p *FileProcessor) Close() error {
	return p.file.Close()
}
