package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"opspilot/internal/event"
)

// NewKafkaWriter creates a producer for the incident events topic.
func NewKafkaWriter(brokers []string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        event.Topic,
		Balancer:     &kafka.Hash{},         // same key (incident ID) → same partition → events stay in order
		RequiredAcks: kafka.RequireAll,      // wait until every in-sync replica has the message
		BatchTimeout: 10 * time.Millisecond, // the default (1s) would delay every publish by up to a second
		WriteTimeout: 5 * time.Second,
	}
}

// CheckKafka connects to the first broker and confirms the topic exists, so we fail fast.
func CheckKafka(ctx context.Context, brokers []string) error {
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka %s: %w", brokers[0], err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(event.Topic)
	if err != nil {
		return fmt.Errorf("read partitions of topic %q (did you create it?): %w", event.Topic, err)
	}
	slog.Info("connected to kafka", "brokers", brokers, "topic", event.Topic, "partitions", len(partitions))
	return nil
}
