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

// CheckKafka connects to the first broker and confirms the topics exist, so we fail fast.
func CheckKafka(ctx context.Context, brokers []string, topics ...string) error {
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka %s: %w", brokers[0], err)
	}
	defer conn.Close()

	for _, topic := range topics {
		partitions, err := conn.ReadPartitions(topic)
		if err != nil {
			return fmt.Errorf("read partitions of topic %q (did you create it?): %w", topic, err)
		}
		slog.Info("connected to kafka", "brokers", brokers, "topic", topic, "partitions", len(partitions))
	}
	return nil
}
