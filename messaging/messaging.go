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
		DependencyType: "EventHub",
		DependencySuccess: true,
	}
	telemetry.TrackDependency(telemetryData)

	return nil
}

// Publish sends a message to the EventHub
func (e *EventHub) Publish(message string) error {
	startTime := time.Now()

	ctx := context.Background() // Create a new context here

	event := eventhub.NewEventFromString(message)
	err := e.Hub.Send(ctx, event)

	if err != nil {
		// Failed to send message
		telemetryData := telemetry.TelemetryData{
			Message: "Messaging::Failed to send message",
			Properties: map[string]string{"Error": err.Error()},
			DependencyType: "EventHub",
			DependencySuccess: false,
			startTime = startTime
			endTime = time.Now()
			}
		telemetry.TrackDependency(telemetryData)
	} else {
		// Successfully sent message, log to App Insights
		telemetryData := telemetry.TelemetryData{
			Message: "Messaging::Message sent",
			DependencyType: "EventHub",
			DependencySuccess: true,
			startTime = startTime
			endTime = time.Now()
		}

		telemetry.TrackDependency(telemetryData)
	}

	return err
}
