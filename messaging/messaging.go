package messaging

import (
	"context"
	"github.com/Azure/azure-event-hubs-go"

	"github.com/microtest/telemetry"
)

var EventHubInstance *EventHub

type EventHub struct {
	Hub *eventhub.Hub
}

// NewEventHub initializes a new EventHub instance
func NewEventHub(connectionString string) error {
	hub, err := eventhub.NewHubFromConnectionString(connectionString)
	if err != nil {
		telemetry.TrackException(err)
		return err
	}
	EventHubInstance = &EventHub{Hub: hub}

	// Log the event to App Insights
	telemetryData := telemetry.TelemetryData{
		Message: "Messaging::EventHub initialized",
		Severity: telemetry.Information,
	}
	telemetry.TrackDependency(telemetryData)

	return nil
}

// Publish sends a message to the EventHub
func (e *EventHub) Publish(message string) error {
	ctx := context.Background() // Create a new context here

	event := eventhub.NewEventFromString(message)
	err := e.Hub.Send(ctx, event)

	if err != nil {
		// Failed to send message
		telemetryData := telemetry.TelemetryData{
			Message: "Messaging::Failed to send message",
			Properties: map[string]string{"Error": err.Error()},
			Severity: telemetry.Error,
		}
		telemetry.TrackDependency(telemetryData)
	} else {
		// Successfully sent message, log to App Insights
		telemetryData := telemetry.TelemetryData{
			Message: "Messaging::Message sent",
			Properties: map[string]string{"Message": message},
			Severity: telemetry.Information,
		}
		telemetry.TrackDependency(telemetryData)
	}

	return err
}
