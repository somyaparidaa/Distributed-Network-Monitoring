package model

import (
	"testing"
	"time"
)

func TestTelemetryEventValidation(t *testing.T) {
	now := time.Now().UTC()

	valid := TelemetryEvent{
		EventID:      "evt-1",
		DeviceID:     "router-01",
		Timestamp:    now,
		CPU:          20.0,
		Memory:       40.0,
		LatencyMS:    15,
		PacketLoss:   0.0,
		InterfaceUp:  true,
		Connectivity: true,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("expected valid telemetry event, got error: %v", err)
	}

	// Missing EventID
	missingID := valid
	missingID.EventID = ""
	if err := missingID.Validate(); err == nil {
		t.Fatal("expected error on empty EventID")
	}

	// Missing DeviceID
	missingDev := valid
	missingDev.DeviceID = "   "
	if err := missingDev.Validate(); err == nil {
		t.Fatal("expected error on empty DeviceID")
	}

	// Zero timestamp
	zeroTime := valid
	zeroTime.Timestamp = time.Time{}
	if err := zeroTime.Validate(); err == nil {
		t.Fatal("expected error on zero Timestamp")
	}
}

func TestHealthEventValidation(t *testing.T) {
	now := time.Now().UTC()

	validStatuses := []string{"HEALTHY", "WARNING", "CRITICAL", "DOWN"}
	for _, st := range validStatuses {
		valid := HealthEvent{
			EventID:       "evt-2",
			DeviceID:      "router-02",
			Timestamp:     now,
			CurrentStatus: st,
			Score:         10,
		}
		if err := valid.Validate(); err != nil {
			t.Fatalf("expected status %s to be valid, got: %v", st, err)
		}
	}

	// Missing EventID
	missingID := HealthEvent{
		EventID:       "",
		DeviceID:      "router-01",
		Timestamp:     now,
		CurrentStatus: "HEALTHY",
	}
	if err := missingID.Validate(); err == nil {
		t.Fatal("expected error on empty EventID")
	}

	// Empty current_status
	emptyStatus := HealthEvent{
		EventID:       "evt-2",
		DeviceID:      "router-01",
		Timestamp:     now,
		CurrentStatus: "  ",
	}
	if err := emptyStatus.Validate(); err == nil {
		t.Fatal("expected error on empty current_status")
	}

	// Invalid current_status
	invalidStatus := HealthEvent{
		EventID:       "evt-2",
		DeviceID:      "router-01",
		Timestamp:     now,
		CurrentStatus: "RANDOM_STATUS",
	}
	if err := invalidStatus.Validate(); err == nil {
		t.Fatal("expected error on unknown current_status")
	}
}
