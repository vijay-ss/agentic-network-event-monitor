package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"

	"go-log-producer/models"
)

type KafkaProducer struct {
	Producer sarama.SyncProducer
	Topic    string
}

func NewKafkaProducer(brokers []string, topic string) (*KafkaProducer, error) {
	config := sarama.NewConfig()

	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	return &KafkaProducer{
		Producer: producer,
		Topic: topic,
	}, nil
}

func (kp *KafkaProducer) PublishEvent(ctx context.Context, event models.LogEntry) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	eventBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event into bytes: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: kp.Topic,
		Key:   sarama.StringEncoder(event.ID),
		Value: sarama.ByteEncoder(eventBytes),
		Timestamp: time.Now(),
	}

	partition, offset, err := kp.Producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	log.Printf("Message sent to partition %d at offset %d\n", partition, offset)
	return nil
}

func (kp *KafkaProducer) PublishEvents(ctx context.Context, events []models.LogEntry) error {
	for i, event := range events {
		log.Printf("Processing log entry %d/%d - ID: %s", i+1, len(events), event.ID)

		err := kp.PublishEvent(ctx, event)
		if err != nil {
			log.Printf("Error publishing event ID %s: %v\n", event.ID, err)
			continue
		}
		raw_event, err := json.Marshal(event)
		log.Printf("Successfully published log item: %s", string(raw_event))
		log.Printf("Successfully published event ID %s\n", event.ID)
	}
	return nil
}

func (kp *KafkaProducer) Close() error {
	return kp.Producer.Close()
}