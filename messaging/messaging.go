package messaging

import (
	"context"
	"time"
	"strconv"

	"github.com/Azure/azure-event-hubs-go"
	"github.com/microtest/telemetry"
)

var EventHubInstance *EventHub

type EventHub struct {
	Hub *eventhub.Hub
}

// NewEventHub initializes a new EventHub instance
func NewEventHub(serviceName string, connectionString string) error {
	startTime := time.Now()

	// Create a new EventHub instance
	hub, err := eventhub.NewHubFromConnectionString(connectionString)
	if err != nil {
		telemetry.TrackException(err)
		return err
	}
	EventHubInstance = &EventHub{Hub: hub}

	// Log the event to App Insights
	telemetryData := telemetry.TelemetryData{
		Message: "Messaging::EventHub initialized by " + serviceName,
		DependencyType: "EventHub",
		DependencySuccess: true,
		StartTime: startTime,
		EndTime: time.Now(),
	}
	telemetry.TrackDependency(telemetryData)

	return nil
}

// Publish sends a message to the EventHub
func (e *EventHub) Publish(message string, messageID string) error {
	startTime := time.Now()

	// Create a new context for the message and send it
	ctx := context.Background()
	event := eventhub.NewEventFromString(message)

	// Add properties to the event data
	event.Properties = map[string]interface{}{
		"messageId": messageID,
	}

	err := e.Hub.Send(ctx, event)

	if err != nil {
		// Failed to send message, log dependency failure to App Insights
		telemetryData := telemetry.TelemetryData{
			Message: "Messaging::Publish::Failed to send message",
			Properties: map[string]string{
				"Error": err.Error(),
				"messageId": messageID,
			},
			DependencyType: "EventHub",
			DependencySuccess: false,
			StartTime: startTime,
			EndTime: time.Now(),
		}
		telemetry.TrackDependency(telemetryData)
	} else {
		// Successfully sent message, log to App Insights
		telemetryData := telemetry.TelemetryData{
			Message: "Messaging::Publish::Message sent",
			Properties: map[string]string{
				"messageId": messageID,
			},
			DependencyType: "EventHub",
			DependencySuccess: true,
			StartTime: startTime,
			EndTime: time.Now(),
		}
		telemetry.TrackDependency(telemetryData)
	}

	return err
}

// Subscribe listens for messages on the EventHub
func (e *EventHub) Subscribe(handler func(string)) error {
	startTime := time.Now()

	// Create a new context for the message and receive it
	ctx := context.Background()
	_, err := e.Hub.Receive(ctx, "$Default", func(ctx context.Context, event *eventhub.Event) error {
		message := string(event.Data)
		handler(message)

		// Log the event to App Insights
		telemetryData := telemetry.TelemetryData{
			Message: "Messaging::Subscribe::Message received from EventHub",
			Properties: map[string]string{
				"content": message,
				"size":    strconv.Itoa(len(event.Data)),  // size of the message in bytes
			},
			Severity: telemetry.Information,
		}
		telemetry.TrackTrace(telemetryData)

		return nil
	})

	if err != nil {
		// Failed to receive message, log dependency failure to App Insights
		telemetryData := telemetry.TelemetryData{
			Message: "Messaging::Subscribe::Failed to receive message",
			Properties: map[string]string{"Error": err.Error()},
			DependencyType: "EventHub",
			DependencySuccess: false,
			StartTime: startTime,
			EndTime: time.Now(),
		}
		telemetry.TrackDependency(telemetryData)
	}

	return err
}
