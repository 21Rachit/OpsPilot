package notifier

import (
	"context"
	"fmt"
	"log/slog"

	"opspilot/internal/consumer"
	"opspilot/internal/event"
)

// NewProcess returns the notifier's work for one event: append a line to this consumer's file.
// The shared consumer loop (internal/consumer) handles retries, duplicates and the DLQ.
func NewProcess(writer *FileWriter) consumer.ProcessFunc {
	return func(ctx context.Context, log *slog.Logger, ev event.Event, partition int) error {
		line := fmt.Sprintf("hello i am a file and consumes from partition %d with event id %s", partition, ev.ID)
		if err := writer.Write(line); err != nil {
			return err
		}
		log.Info("notifier: event written", "file", writer.Path())
		return nil
	}
}
