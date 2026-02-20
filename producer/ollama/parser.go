package ollama

import (
	"encoding/json"
	"fmt"
	"go-log-producer/models"
	"log"
	"strings"
)

func ExtractJSON(text string) string {
	text = strings.TrimSpace(text)
	
	if strings.HasPrefix(text, "```json") || strings.HasPrefix(text, "```") {
		text = strings.TrimPrefix(text, "```json")
		text = strings.TrimPrefix(text, "```")
	}

	if strings.HasSuffix(text, "```") {
		text = strings.TrimSuffix(text, "```")
	}
	
	return strings.TrimSpace(text)
}

// ParseLogEntries parses a JSON array string into a slice of LogEntry objects
func ParseLogEntries(jsonData string) ([]models.LogEntry, error) {
	var rawItems []json.RawMessage
	var logEntries []models.LogEntry

	if err := json.Unmarshal([]byte(jsonData), &rawItems); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON array: %w", err)
	}

	for i, raw := range rawItems {
		var entry models.LogEntry
		if err := json.Unmarshal(raw, &entry); err != nil {
			log.Printf("failed to unmarshal element at index %d: %s \n%v", i, string(raw), err)
			continue
		}

		logEntries = append(logEntries, entry)
	}

	return logEntries, nil
}
