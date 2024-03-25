package messaging

import (
	"context"
	"github.com/Azure/azure-event-hubs-go"
)

var EventHubInstance *EventHub

type EventHub struct {
	Hub *eventhub.Hub
}

// NewEventHub initializes a new EventHub instance
func NewEventHub(connectionString string) error {
	hub, err := eventhub.NewHubFromConnectionString(connectionString)
	if err != nil {
		return err
	}
	EventHubInstance = &EventHub{Hub: hub}
	return nil
}

// Publish sends a message to the EventHub
func (e *EventHub) Publish(message string) error {
	ctx := context.Background() // Create a new context here

	event := eventhub.NewEventFromString(message)
	err := e.Hub.Send(ctx, event)
	return err
}
