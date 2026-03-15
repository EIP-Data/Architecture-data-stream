package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

// Event represents a user event
type Event struct {
	EventID   string    `json:"event_id"`
	UserID    string    `json:"user_id"`
	EventType string    `json:"event_type"`
	Timestamp string    `json:"timestamp"`
	Data      EventData `json:"data"`
}

// EventData contains additional event information
type EventData struct {
	Browser string `json:"browser"`
	OS      string `json:"os"`
	Value   int    `json:"value"`
}

// DataStreamProducer handles Kafka and Cassandra operations
type DataStreamProducer struct {
	kafkaWriter *kafka.Writer
	cassandra   *gocql.Session
}

// NewDataStreamProducer creates a new producer instance
func NewDataStreamProducer(kafkaBrokers []string, cassandraHosts []string) (*DataStreamProducer, error) {
	// Initialize Kafka Writer
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaBrokers...),
		Topic:                  "user-events",
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}

	// Initialize Cassandra Connection
	session, err := initCassandra(cassandraHosts)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Cassandra: %w", err)
	}

	return &DataStreamProducer{
		kafkaWriter: writer,
		cassandra:   session,
	}, nil
}

// initCassandra initializes the Cassandra connection and schema
func initCassandra(hosts []string) (*gocql.Session, error) {
	log.Println("Connecting to Cassandra...")

	cluster := gocql.NewCluster(hosts...)
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 10 * time.Second
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 3}

	// Retry connection for up to 1 minute
	var session *gocql.Session
	var err error

	for i := 0; i < 12; i++ {
		session, err = cluster.CreateSession()
		if err == nil {
			break
		}
		log.Printf("Cassandra not ready yet, retrying... (%d/12)", i+1)
		time.Sleep(5 * time.Second)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to Cassandra: %w", err)
	}

	// Create keyspace
	if err := session.Query(`
		CREATE KEYSPACE IF NOT EXISTS datastream
		WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}
	`).Exec(); err != nil {
		return nil, fmt.Errorf("failed to create keyspace: %w", err)
	}

	// Switch to keyspace
	session.Close()
	cluster.Keyspace = "datastream"
	session, err = cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to keyspace: %w", err)
	}

	// Create table
	if err := session.Query(`
		CREATE TABLE IF NOT EXISTS message_metadata (
			message_id uuid PRIMARY KEY,
			topic text,
			timestamp timestamp,
			partition int,
			offset bigint,
			key text,
			value_size int
		)
	`).Exec(); err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	log.Println("Cassandra initialized successfully!")
	return session, nil
}

// ProduceMessage sends a message to Kafka and stores metadata in Cassandra
func (p *DataStreamProducer) ProduceMessage(ctx context.Context, key string, event Event) error {
	messageID := uuid.New()
	timestamp := time.Now()

	// Serialize event to JSON
	valueBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Send to Kafka
	msg := kafka.Message{
		Key:   []byte(key),
		Value: valueBytes,
		Time:  timestamp,
	}

	err = p.kafkaWriter.WriteMessages(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to write message to Kafka: %w", err)
	}

	// Store metadata in Cassandra
	if err := p.cassandra.Query(`
		INSERT INTO message_metadata (message_id, topic, timestamp, partition, offset, key, value_size)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, messageID, "user-events", timestamp, 0, 0, key, len(valueBytes)).Exec(); err != nil {
		log.Printf("Warning: failed to store metadata in Cassandra: %v", err)
	}

	log.Printf("Message sent - ID: %s, Key: %s, Event: %s", messageID, key, event.EventType)
	return nil
}

// Close closes all connections
func (p *DataStreamProducer) Close() error {
	if err := p.kafkaWriter.Close(); err != nil {
		return err
	}
	p.cassandra.Close()
	return nil
}

func main() {
	log.Println("Starting Kafka Producer (Go)...")

	// Initialize producer
	producer, err := NewDataStreamProducer(
		[]string{"localhost:9092"},
		[]string{"localhost"},
	)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	ctx := context.Background()
	eventTypes := []string{"login", "purchase", "view", "logout"}

	// Produce sample messages
	for i := 0; i < 10; i++ {
		event := Event{
			EventID:   uuid.New().String(),
			UserID:    fmt.Sprintf("user_%d", i%5),
			EventType: eventTypes[i%4],
			Timestamp: time.Now().Format(time.RFC3339),
			Data: EventData{
				Browser: "Firefox",
				OS:      "MacOS",
				Value:   i * 100,
			},
		}

		err := producer.ProduceMessage(ctx, fmt.Sprintf("user_%d", i%5), event)
		if err != nil {
			log.Printf("Error producing message: %v", err)
			continue
		}

		time.Sleep(1 * time.Second)
	}

	log.Println("\nAll messages sent successfully!")
}
