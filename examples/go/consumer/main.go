package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/gocql/gocql"
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

// DataStreamConsumer handles Kafka and Cassandra operations
type DataStreamConsumer struct {
	kafkaReader *kafka.Reader
	cassandra   *gocql.Session
}

// NewDataStreamConsumer creates a new consumer instance
func NewDataStreamConsumer(kafkaBrokers []string, cassandraHosts []string, groupID string) (*DataStreamConsumer, error) {
	// Initialize Kafka Reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        kafkaBrokers,
		Topic:          "user-events",
		GroupID:        groupID,
		MinBytes:       10e1, // 100B
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset,
	})

	// Initialize Cassandra Connection
	session, err := initCassandra(cassandraHosts)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize Cassandra: %w", err)
	}

	return &DataStreamConsumer{
		kafkaReader: reader,
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

	// Create tables
	if err := session.Query(`
		CREATE TABLE IF NOT EXISTS user_events (
			event_id uuid,
			user_id text,
			event_type text,
			timestamp timestamp,
			browser text,
			os text,
			value int,
			consumed_at timestamp,
			PRIMARY KEY (user_id, timestamp, event_id)
		) WITH CLUSTERING ORDER BY (timestamp DESC)
	`).Exec(); err != nil {
		return nil, fmt.Errorf("failed to create user_events table: %w", err)
	}

	if err := session.Query(`
		CREATE TABLE IF NOT EXISTS user_event_counts (
			user_id text PRIMARY KEY,
			total_events counter
		)
	`).Exec(); err != nil {
		return nil, fmt.Errorf("failed to create user_event_counts table: %w", err)
	}

	log.Println("Cassandra initialized successfully!")
	return session, nil
}

// StoreEvent stores the consumed event in Cassandra
func (c *DataStreamConsumer) StoreEvent(event Event) error {
	// Parse timestamp
	timestamp, err := time.Parse(time.RFC3339, event.Timestamp)
	if err != nil {
		timestamp = time.Now()
	}

	// Insert event into user_events table
	if err := c.cassandra.Query(`
		INSERT INTO user_events (event_id, user_id, event_type, timestamp, browser, os, value, consumed_at)
		VALUES (uuid(), ?, ?, ?, ?, ?, ?, toTimestamp(now()))
	`, event.UserID, event.EventType, timestamp, event.Data.Browser, event.Data.OS, event.Data.Value).Exec(); err != nil {
		return fmt.Errorf("failed to insert event: %w", err)
	}

	// Update counter for user
	if err := c.cassandra.Query(`
		UPDATE user_event_counts
		SET total_events = total_events + 1
		WHERE user_id = ?
	`, event.UserID).Exec(); err != nil {
		return fmt.Errorf("failed to update counter: %w", err)
	}

	log.Printf("Stored event for user %s - Type: %s", event.UserID, event.EventType)
	return nil
}

// Consume starts consuming messages from Kafka
func (c *DataStreamConsumer) Consume(ctx context.Context) error {
	log.Println("\nStarting to consume messages... (Press Ctrl+C to stop)")

	for {
		msg, err := c.kafkaReader.ReadMessage(ctx)
		if err != nil {
			if err == context.Canceled {
				log.Println("Context canceled, stopping consumer...")
				break
			}
			log.Printf("Error reading message: %v", err)
			continue
		}

		log.Println("\n--- New Message ---")
		log.Printf("Topic: %s", msg.Topic)
		log.Printf("Partition: %d", msg.Partition)
		log.Printf("Offset: %d", msg.Offset)
		log.Printf("Key: %s", string(msg.Key))

		// Deserialize event
		var event Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("Error deserializing message: %v", err)
			continue
		}

		log.Printf("Event: %+v", event)

		// Store in Cassandra
		if err := c.StoreEvent(event); err != nil {
			log.Printf("Error storing event: %v", err)
		}
	}

	return nil
}

// GetUserStats retrieves statistics for a user from Cassandra
func (c *DataStreamConsumer) GetUserStats(userID string) (int64, error) {
	var count int64
	if err := c.cassandra.Query(`
		SELECT total_events FROM user_event_counts WHERE user_id = ?
	`, userID).Scan(&count); err != nil {
		if err == gocql.ErrNotFound {
			return 0, nil
		}
		return 0, err
	}
	return count, nil
}

// Close closes all connections
func (c *DataStreamConsumer) Close() error {
	if err := c.kafkaReader.Close(); err != nil {
		return err
	}
	c.cassandra.Close()
	return nil
}

func main() {
	log.Println("Starting Kafka Consumer (Go)...")

	// Initialize consumer
	consumer, err := NewDataStreamConsumer(
		[]string{"localhost:9092"},
		[]string{"localhost"},
		"go-consumer-group",
	)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Start consuming
	ctx := context.Background()
	if err := consumer.Consume(ctx); err != nil {
		log.Fatalf("Error consuming messages: %v", err)
	}
}
