package event

import (
	"encoding/json"
	"time"
)

// Topic is the Kafka topic all incident events are published to.
const Topic = "incident.events"

// DLQTopic receives events a consumer gave up on, so they can be inspected and replayed.
const DLQTopic = "incident.events.dlq"

// Event types.
const (
	IncidentCreated = "incident.created"
	IncidentUpdated = "incident.updated"
	IncidentDeleted = "incident.deleted"
)

// Event is the message other services receive from Kafka.
type Event struct {
	ID         string          `json:"event_id"` // unique per event: lets consumers skip duplicates
	Type       string          `json:"type"`
	IncidentID int             `json:"incident_id"`
	OccurredAt time.Time       `json:"occurred_at"`
	Data       json.RawMessage `json:"data"` // the incident as it was after the change
}
