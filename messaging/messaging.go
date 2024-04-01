package messaging

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/microtest/telemetry"
)

// EventHubInstance is a global instance of the EventHub
var EventHubInstance *EventHub

// EventHub represents the structure of an EventHub
type EventHub struct {
	Hub          *eventhub.Hub
	EventHubName string
}

// Message represents the structure of a message
type Message struct {
	Payload   string `json:"payload"`
	MessageId string `json:"messageId"`
}

// Function to get the EventHub name from the connection string
func getEventHubName(connectionString string) string {
	// Connection string format: Endpoint=sb://<NAMESPACE>.servicebus.windows.net/;SharedAccessKeyName=<KEYNAME
	// ;SharedAccessKey=<KEY>;EntityPath=<EVENTHUBNAME>
	// Split the connection string by ;
	parts := strings.Split(connectionString, ";")
	for _, part := range parts {
		if strings.HasPrefix(part, "EntityPath=") {
			return strings.Split(part, "=")[1]
		}
	}
	return ""
}

// NewEventHub initializes a new EventHub instance
func NewEventHub(serviceName string, connectionString string) error {
	startTime := time.Now()

	// Create a new EventHub instance
	hub, err := eventhub.NewHubFromConnectionString(connectionString)
	if err != nil {
		telemetry.TrackException(err, telemetry.Error, map[string]string{"Error": err.Error(), "Message": "Failed to create new event hub instance"})
		return err
	}
	EventHubInstance = &EventHub{Hub: hub, EventHubName: getEventHubName(connectionString)}

	// Log the dependency to App Insights (success)
	telemetry.TrackDependency("New event hub initialized", serviceName, "EventHub", EventHubInstance.EventHubName, true, startTime, time.Now(), nil, "")

	return nil
}

// Publish sends a message to the EventHub
func (e *EventHub) Publish(serviceName string, operationID string, msg Message) error {
	startTime := time.Now()

	// Create a new context for the message
	ctx := context.Background()

	// Convert the message to JSON
	jsonData, err := json.Marshal(msg)
	if err != nil {
		// Failed to marshal message, log dependency failure to App Insights
		telemetry.TrackDependency("Publish::Failed to marshal message", serviceName, "EventHub", e.EventHubName, false, startTime, time.Now(), map[string]string{"Error": err.Error()}, operationID)
		return err
	}

	// Create a new EventHub event
	event := eventhub.NewEventFromString(string(jsonData))

	// Send the message to the EventHub
	errHub := e.Hub.Send(ctx, event)

	if errHub != nil {
		// Failed to send message, log dependency failure to App Insights
		telemetry.TrackDependency("Publish::Failed to send message", serviceName, "EventHub", e.EventHubName, false, startTime, time.Now(), map[string]string{"Error": errHub.Error(), "messageId": msg.MessageId}, operationID)
	} else {
		// Successfully sent message, log to App Insights
		telemetry.TrackDependency("Publish::Successfully sent message", serviceName, "EventHub", e.EventHubName, true, startTime, time.Now(), map[string]string{"messageId": msg.MessageId}, operationID)
	}

	return errHub
}

// ListenForMessages starts listening for messages on the provided hub and partition
func (e *EventHub) ListenForMessages(serviceName string, partitionID string, messages chan<- Message) error {
	startTime := time.Now()

	// Create a new context for the message and receive it
	ctx := context.Background()

	// Start receiving messages from the specified partition
	_, err := e.Hub.Receive(ctx, partitionID, func(ctx context.Context, event *eventhub.Event) error {
		// Unmarshal the JSON message received
		var msg Message
		err := json.Unmarshal(event.Data, &msg)
		if err != nil {
			// Log the error using telemetry.TrackDependency
			telemetry.TrackDependency("Failed to unmarshal message", serviceName, "EventHub", e.EventHubName, false, startTime, time.Now(), map[string]string{"partitionId": partitionID, "Error": err.Error()}, "")
			return err
		}

		// Send the message to the consumer
		for {
			select {
			case messages <- msg:
				// Successfully received message, log to App Insights
				telemetry.TrackDependency("Successfully received message id "+msg.MessageId+" from event hub from partition id "+partitionID, serviceName, "EventHub", e.EventHubName, true, startTime, time.Now(), map[string]string{"partitionId": partitionID, "content": msg.Payload, "messageId": msg.MessageId, "msg": string(event.Data), "size": strconv.Itoa(len(event.Data))}, "")
				// Message successfully sent, break the loop
				return nil
			default:
				// If the messages channel is full, wait and retry
				time.Sleep(100 * time.Millisecond) // Adjust the sleep duration as needed
			}
		}
	})

	if err != nil {
		// Log the error using telemetry.TrackDependency
		telemetry.TrackDependency("Error receiving message from partition", serviceName, "EventHub", e.EventHubName, false, startTime, time.Now(), map[string]string{"partitionId": partitionID, "Error": err.Error()}, "")
	}

	return err
}
