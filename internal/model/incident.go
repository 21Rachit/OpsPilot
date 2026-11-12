package model

import (
	"errors"
	"time"
)

type Incident struct {
	ID        int       `json:"id"`
	Title     string    `json:"title"`
	Severity  string    `json:"severity"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

var ValidSeverities = map[string]bool{"low": true, "medium": true, "high": true, "critical": true}

var ValidStatuses = map[string]bool{"open": true, "acknowledged": true, "resolved": true}

// Validate checks the fields a client sends. Status is only checked when checkStatus is
// true, because new incidents always start as open. Both the HTTP and the gRPC API use it,
// so the rules can't drift apart.
func Validate(title, severity, status string, checkStatus bool) error {
	if title == "" {
		return errors.New("title is required")
	}
	if !ValidSeverities[severity] {
		return errors.New("severity must be low, medium, high or critical")
	}
	if checkStatus && !ValidStatuses[status] {
		return errors.New("status must be open, acknowledged or resolved")
	}
	return nil
}
