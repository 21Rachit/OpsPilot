package model

import "time"

type Incident struct {
	ID        int       `json:"id"`
	Title     string    `json:"title"`
	Severity  string    `json:"severity"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

var ValidSeverities = map[string]bool{"low": true, "medium": true, "high": true, "critical": true}

var ValidStatuses = map[string]bool{"open": true, "acknowledged": true, "resolved": true}
