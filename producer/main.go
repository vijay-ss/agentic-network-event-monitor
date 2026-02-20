package main

import (
	"fmt"
	"go-log-producer/generator"
	"go-log-producer/kafka"
	"go-log-producer/ollama"
	"log"
	"os"
	"time"
)

type Event struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Message   string    `json:"message"`
	CreatedAt time.Time `json:"created_at"`
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func generateID() string {
	return time.Now().Format("20060102150405.000000000")
}

func main() {
	brokers := []string{getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")}
	topic := os.Getenv("KAFKA_TOPIC")
	ollamaHost := fmt.Sprintf("http://%s", getEnv("OLLAMA_LOCAL", "OLLAMA_HOST"))

	for _, e := range os.Environ() {
		fmt.Println(e)
	}

	content, err := os.ReadFile("prompts/security_log_prompt_production.txt")
	if err != nil {
		log.Fatalf("Error reading prompt file: %v", err)
	}

	// content_instruction := string(content) + "\n-------\nReturn a response with 10 examples in a JSON array format only. Dont include any additional markdown or text characters. I only want the JSON array. I want to use this in Golang."
	batchSize := 10
	prompt := fmt.Sprintf("%s\n------\nReturn a response with %d examples in a JSON array format only. Dont include any additional markdown or text characters. I only want the JSON array.", content, batchSize)

	// ollama_host := fmt.Sprintf("http://%s", getEnv("OLLAMA_HOST", "localhost:11434"))
	client := ollama.NewClient(ollamaHost)

	kafkaProducer, err := kafka.NewKafkaProducer(brokers, topic)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer kafkaProducer.Close()

	promptRequest := ollama.ChatRequest{
			Model: "gemma3:4b",
			Options: map[string]interface{}{
				"num_ctx": 8192, //4096 default
			},
			Messages: []ollama.Message{
				// {Role: "system", Content: "You are a network security logger."},
				{Role: "user", Content: prompt},
			},
		}

	// generator.GenerateLogsInBatches(
	// 	client,
	// 	&promptRequest,
	// 	kafkaProducer,
	// 	content_instruction,
	// 	20,
	// 	10,
	// )
	// if err != nil {
	// 	log.Fatalf("Failed to batch send logs: %v", err)
	// }

	err = os.MkdirAll("./overflow", os.ModePerm)
	if err != nil {
		log.Fatal("Error creating directory")
	}
	err = generator.StreamLogsWithDiskOverflow(
		client,
		&promptRequest,
		kafkaProducer,
		1000,
		batchSize,
		"./overflow/overflow.jsonl",
	)
	if err != nil {
		log.Fatalf("Failed streaming: %v", err)
	}
}
