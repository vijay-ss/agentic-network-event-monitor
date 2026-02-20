package generator

import (
	"context"
	"log"
	"time"
	"go-log-producer/ollama"
	"go-log-producer/kafka"
)

func GenerateLogsInBatches(
	llmClient *ollama.Client,
	llmRequest *ollama.ChatRequest,
	kafkaProducer *kafka.KafkaProducer,
	prompt string,
	totalLogs int,
	batchSize int,
) error {
	defer kafkaProducer.Close()

	totalBatches := (totalLogs + batchSize - 1) / batchSize
	
	log.Printf("Generating %d logs in %d batches of ~%d logs each", 
		totalLogs, totalBatches, batchSize)
	
	totalSuccess := 0
	totalFailures := 0
	startTime := time.Now()
	
	for batch := 0; batch < totalBatches; batch++ {
		logsInBatch := batchSize
		if batch == totalBatches-1 {
			logsInBatch = totalLogs - (batch * batchSize)
		}
		
		log.Printf("\n--- Batch %d/%d: Requesting %d logs ---", batch+1, totalBatches, logsInBatch)
		
		batchStartTime := time.Now()
		logsResp, err := llmClient.Chat(context.Background(), *llmRequest)
		if err != nil {
			log.Printf("ERROR: Batch %d failed: %v", batch+1, err)
			totalFailures += logsInBatch
			continue
		}

		parsedLogs := ollama.ExtractJSON(logsResp.Message.Content)
		logEntries, err := ollama.ParseLogEntries(parsedLogs)
		if err != nil {
			log.Fatalf("Failed to parse JSON: %v", err)
		}
		log.Printf("Received %d events from Ollama response", len(logEntries))
		
		batchDuration := time.Since(batchStartTime)
		log.Printf("✓ Generated %d logs in %.2f seconds", 
			len(logEntries), batchDuration.Seconds())
		
		for i, logEntry := range logEntries {
			if err := kafkaProducer.PublishEvent(context.Background(), logEntry); err != nil {
				log.Printf("Failed to send log %d: %v", i+1, err)
				totalFailures++
			} else {
				totalSuccess++
			}
		}
		
		log.Printf("Progress: %d/%d logs sent successfully", totalSuccess, totalLogs)
		
		// Rate limiting
		if batch < totalBatches-1 {
			sleepDuration := 2 * time.Second
			log.Printf("Sleeping %v before next batch...", sleepDuration)
			time.Sleep(sleepDuration)
		}
	}
	
	totalDuration := time.Since(startTime)
	
	log.Printf("\n=== FINAL SUMMARY ===")
	log.Printf("Total logs requested: %d", totalLogs)
	log.Printf("Successfully sent to Kafka: %d", totalSuccess)
	log.Printf("Failures: %d", totalFailures)
	log.Printf("Total time: %.2f seconds", totalDuration.Seconds())
	log.Printf("Average: %.2f logs/second", float64(totalSuccess)/totalDuration.Seconds())
	
	return nil
}