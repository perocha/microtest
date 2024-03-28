package messaging

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go"
	"github.com/microtest/telemetry"
)

var EventHubInstance *EventHub

type EventHub struct {
	Hub *eventhub.Hub
}

// Message represents the structure of a message
type Message struct {
	Payload   string `json:"payload"`
	MessageId string `json:"messageId"`
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
		Message:           "Messaging::EventHub initialized by " + serviceName,
		DependencyType:    "EventHub",
		DependencySuccess: true,
		StartTime:         startTime,
		EndTime:           time.Now(),
	}
	telemetry.TrackDependency(telemetryData)

	return nil
}

// Publish sends a message to the EventHub
func (e *EventHub) Publish(msg Message) error {
	startTime := time.Now()

	// Create a new context for the message
	ctx := context.Background()

	// Convert the message to JSON
	jsonData, err := json.Marshal(msg)
	if err != nil {
		// Failed to marshal message, log dependency failure to App Insights
		telemetryData := telemetry.TelemetryData{
			Message:           "Messaging::Publish::Failed to marshal message",
			Properties:        map[string]string{"Error": err.Error()},
			DependencyType:    "EventHub",
			DependencySuccess: false,
			StartTime:         startTime,
			EndTime:           time.Now(),
		}
		telemetry.TrackDependency(telemetryData)
		return err
	}

	// Create a new EventHub event
	event := eventhub.NewEventFromString(string(jsonData))

	// Send the message to the EventHub
	errHub := e.Hub.Send(ctx, event)

	if errHub != nil {
		// Failed to send message, log dependency failure to App Insights
		telemetryData := telemetry.TelemetryData{
			Message: "Messaging::Publish::Failed to send message to EventHub",
			Properties: map[string]string{
				"Error":     err.Error(),
				"messageId": msg.MessageId,
			},
			DependencyType:    "EventHub",
			DependencySuccess: false,
			StartTime:         startTime,
			EndTime:           time.Now(),
		}
		telemetry.TrackDependency(telemetryData)
	} else {
		// Successfully sent message, log to App Insights
		telemetryData := telemetry.TelemetryData{
			Message: "Messaging::Publish::Message sent to EventHub",
			Properties: map[string]string{
				"messageId": msg.MessageId,
			},
			DependencyType:    "EventHub",
			DependencySuccess: true,
			StartTime:         startTime,
			EndTime:           time.Now(),
		}
		telemetry.TrackDependency(telemetryData)
	}

	return errHub
}

// Subscribe listens for messages on the EventHub
func (e *EventHub) Subscribe(handler func(Message)) error {
	startTime := time.Now()

	// Create a new context for the message and receive it
	ctx := context.Background()

	_, err := e.Hub.Receive(ctx, "0", func(ctx context.Context, event *eventhub.Event) error {
		// Unmarshal the JSON message received
		var msg Message
		err := json.Unmarshal(event.Data, &msg)
		if err != nil {
			// Failed to unmarshal message, log dependency failure to App Insights
			telemetryData := telemetry.TelemetryData{
				Message:           "Messaging::Subscribe::Failed to unmarshal message",
				Properties:        map[string]string{"Error": err.Error()},
				DependencyType:    "EventHub",
				DependencySuccess: false,
				StartTime:         startTime,
				EndTime:           time.Now(),
			}
			telemetry.TrackDependency(telemetryData)
			return nil
		}

		handler(msg)

		// Log the event to App Insights
		telemetryData := telemetry.TelemetryData{
			Message: "Messaging::Subscribe::Message received from EventHub",
			Properties: map[string]string{
				"content":   msg.Payload,
				"messageId": msg.MessageId,
				"size":      strconv.Itoa(len(event.Data)), // size of the message in bytes
			},
			DependencyType:    "EventHub",
			DependencySuccess: true,
			StartTime:         startTime,
			EndTime:           time.Now(),
		}
		telemetry.TrackDependency(telemetryData)

		return nil
	})

	if err != nil {
		// Failed to receive message
		telemetryData := telemetry.TelemetryData{
			Message:           "Messaging::Subscribe::Failed to receive message from EventHub",
			Properties:        map[string]string{"Error": err.Error()},
			DependencyType:    "EventHub",
			DependencySuccess: false,
			StartTime:         startTime,
			EndTime:           time.Now(),
		}
		telemetry.TrackDependency(telemetryData)
	}

	return err
}
