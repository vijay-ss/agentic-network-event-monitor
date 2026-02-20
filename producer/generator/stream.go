package generator

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"go-log-producer/kafka"
	"go-log-producer/models"
	"go-log-producer/ollama"
	"log"
	"os"
	"sync"
	"os/signal"
	"syscall"
	"time"
)

// Producer: Generate logs indefinitely and send to buffer or disk
func StreamLogsWithDiskOverflow(
	llmClient *ollama.Client,
	llmRequest *ollama.ChatRequest,
	kafkaProducer *kafka.KafkaProducer,
	targetLogsPerHour int,
	maxQueueSize int,
	overflowFile string,
) error {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	logBuffer := make(chan models.LogEntry, maxQueueSize)

	var mu sync.Mutex
	var stats struct {
		generated int64
		sentToBuffer int64
		savedToDisk int64
		sentToKafka int64
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer log.Println("Producer: stopped")

		batchSize := 10
		interval := time.Hour / time.Duration(targetLogsPerHour/batchSize)

		log.Printf("--- Starting stream log generation: %d logs/hour, %d per batch every %v ---",
			targetLogsPerHour, batchSize, interval)
		
		generateBatch := func() bool {
			log.Println("Making call to Ollama")
			logsResp, err := llmClient.Chat(context.Background(), *llmRequest)
			if err != nil {
				log.Printf("ERROR: failed to generate logs: %v", err)
				return false
			}

			parsedLogs := ollama.ExtractJSON(logsResp.Message.Content)
			logEntries, err := ollama.ParseLogEntries(parsedLogs)
			if err != nil {
				log.Printf("Failed to parse JSON: %v", err)
				return false
			}
			log.Printf("Received %d events from Ollama response", len(logEntries))

			mu.Lock()
			stats.generated += int64(len(logEntries))
			mu.Unlock()

			for _, logEntry := range logEntries {
				select {
				case logBuffer <- logEntry:
					mu.Lock()
					stats.sentToBuffer++
					mu.Unlock()
				default:
					// if buffer full
					if err := saveLogToDisk(overflowFile, logEntry); err != nil {
						log.Printf("ERROR: Failed to save overflow file to disk: %v", err)
					} else {
						mu.Lock()
						stats.savedToDisk++
						mu.Unlock()
						log.Printf("Buffer full (%d/%d), saved log to disk: %s",
							len(logBuffer), maxQueueSize, logEntry.ID)
					}
				}
			}

			mu.Lock()
			log.Printf("Stats: Generated=%d, InBuffer=%d, OnDisk=%d, SentToKafka=%d",
				stats.generated, stats.sentToBuffer, stats.savedToDisk, stats.sentToKafka)
			mu.Unlock()

			return true
		}

		log.Println("Generating first batch...")
		generateBatch()

		log.Printf("Next batch in %v", interval)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				log.Println("Scheduled batch starting...")
				generateBatch()
				log.Printf("Next batch in %v", interval)

			case <-ctx.Done():
				log.Println("Producer: shutdown signal received, stopping...")
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer log.Println("Consumer: stopped")

		for logEntry := range logBuffer {
			if err := kafkaProducer.PublishEvent(context.Background(),logEntry); err != nil {
				log.Printf("Kafka send error: %v", err)
			} else {
				mu.Lock()
				stats.sentToKafka++
				mu.Unlock()
			}
		}

		for {
			select {
			case logEntry, ok := <-logBuffer:
				if !ok {
					return
				}
				if err := kafkaProducer.PublishEvent(context.Background(),logEntry); err != nil {
					log.Printf("Kafka send error: %v", err)
				} else {
					mu.Lock()
					stats.sentToKafka++
					mu.Unlock()
				}
			case <-ctx.Done():
				log.Printf("Consumer: draining %d remaining logs from buffer...", len(logBuffer))
				for len(logBuffer) > 0 {
					logEntry := <-logBuffer
					if err := kafkaProducer.PublishEvent(context.Background(), logEntry); err != nil {
						log.Printf("Kafka error during drain: %v", err)
					} else {
						mu.Lock()
						stats.sentToKafka++
						mu.Unlock()
					}

				}
				log.Println("Comsumer: buffer drained")
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer log.Println("Monitor: stopped")

		replayOverflow := func() {
			if fileSize, err := getFileSize(overflowFile); err == nil && fileSize > 0 {
				log.Printf("Replaying overflow file (%d bytes)...", fileSize)
				replayed, err := replayLogsFromDisk(overflowFile, kafkaProducer, logBuffer)
				if err != nil {
					log.Printf("ERROR: Failed to replay logs: %v", err)
				} else if replayed > 0 {
					log.Printf("Replayed %d logs from disk", replayed)
					mu.Lock()
					stats.savedToDisk -= int64(replayed)
					stats.sentToKafka += int64(replayed)
					mu.Unlock()
				}
			}
		}

		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				replayOverflow()

			case <-ctx.Done():
				log.Println("Monitor: final overflow replay before shutdown...")
				replayOverflow()
				return
			}
		}
	}()

	sig := <-quit
	log.Printf("Received signal: %s - shutting down...", sig)

	cancel()

	shutdownComplete := make(chan struct{})
	go func() {
		wg.Wait()
		close(shutdownComplete)
	}()

	select {
	case <-shutdownComplete:
		log.Println("All goroutines stopped cleanly")

	case <-time.After(30 * time.Second):
		log.Println("Shutdown timed out after 30s - some logs may not have been sent")
	}

	mu.Lock()
	log.Printf("\n=== FINAL STATS ===")
	log.Printf("Generated:     %d logs", stats.generated)
	log.Printf("In Buffer:     %d logs", stats.sentToBuffer)
	log.Printf("Sent to Kafka: %d logs", stats.sentToKafka)
	log.Printf("On disk:       %d logs", stats.savedToDisk)

	if stats.savedToDisk > 0 {
		log.Printf("%d logs remain in overflow file: %s", stats.savedToDisk, overflowFile)
		log.Printf("   Run replayLogsFromDisk() on next startup to send them")
	}
	mu.Unlock()

	log.Println("Shutdown complete")
	return nil
}

// saveLogToDisk appends a log entry to the overflow file
func saveLogToDisk(filename string, log models.LogEntry) error {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open overflow file: %w", err)
	}
	defer f.Close()

	logJSON, err := json.Marshal(log)
	if err != nil {
		return fmt.Errorf("failed to marshal log: %w", err)
	}

	_, err = f.WriteString(string(logJSON) + "\n")
	if err != nil {
		return fmt.Errorf("failed to write to file: %w", err)
	}

	return nil
}

// replayLogsFromDisk reads logs from overflow file and sends to Kafka
// It uses the buffer if available, otherwise sends directly to Kafka
func replayLogsFromDisk(
	fileName string,
	kafkaProducer *kafka.KafkaProducer,
	logBuffer chan models.LogEntry,
) (int, error) {
	file, err := os.Open(fileName)
	if err != nil{
		return 0, fmt.Errorf("failed to open overflow file: %w", err)
	}
	defer file.Close()

	tempFile := fileName + ".tmp"
	temp, err := os.Create(tempFile)
	if err != nil {
		return 0, fmt.Errorf("failed to create temp file: %w", err)
	}
	defer temp.Close()

	replayCount := 0
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		var logEntry models.LogEntry
		if err := json.Unmarshal(scanner.Bytes(), &logEntry); err != nil {
			log.Printf("ERROR: Failed to parse log from disk: %v", err)
			continue
		}

		// try to send to kafka
		select {
		case logBuffer <- logEntry:
		replayCount++
		default:
			// buffer still full - save back to temp file
			temp.WriteString(scanner.Text() + "\n")
		}
	}

	if err := scanner.Err(); err != nil {
		return replayCount, fmt.Errorf("error reading overflow file: %w", err)
	}

	// Replace original file with temp file (contains unsent logs)
	if err := os.Rename(tempFile, fileName); err != nil {
		return replayCount, fmt.Errorf("failed to update overflow file: %w", err)
	}

	if fileSize, _ := getFileSize(fileName); fileSize == 0 {
		os.Remove(fileName)
	}

	return replayCount, nil
}

func getFileSize(fileName string) (int64, error) {
	info, err := os.Stat(fileName)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}