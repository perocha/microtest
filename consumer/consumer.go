package main

import (
	"os"
	"time"

	"github.com/microtest/messaging"
	"github.com/microtest/telemetry"
)

// Method consumeMessages subscribes to the event hub and consumes messages
func consumeMessages() {
	// Subscribe to the event hub
	err := messaging.EventHubInstance.Subscribe("Consumer subscribe", func(msg messaging.Message) {
		// Log the event to App Insights
		telemetry.TrackTrace("Consumer::Message received from EventHub", telemetry.Information, map[string]string{"payload": msg.Payload, "messageId": msg.MessageId})
	})

	// Check if there was an error subscribing to the event hub
	if err != nil {
		telemetry.TrackTrace("Consumer::Failed to subscribe to EventHub", telemetry.Error, map[string]string{"Error": err.Error()})
	}
}

// New consume method that gets one message from eventhub and returns, so we can do something with the message
func consumeMessage() {
	// Get one message from the event hub
	msg, err := messaging.EventHubInstance.GetMessage("Consumer get message")
	if err != nil {
		telemetry.TrackTrace("Consumer::Failed to get message from EventHub", telemetry.Error, map[string]string{"Error": err.Error()})
		return
	}

	// Log the event to App Insights
	telemetry.TrackTrace("Consumer::Message received from EventHub", telemetry.Information, map[string]string{"payload": msg.Payload, "messageId": msg.MessageId})
}

func main() {
	// Initialize telemetry
	telemetry.InitTelemetry("Consumer")

	// Initialize a new EventHub instance
	eventHubConnectionString := os.Getenv("EVENTHUB_CONSUMER_CONNECTION_STRING")
	err := messaging.NewEventHub("Consumer init", eventHubConnectionString)
	if err != nil {
		telemetry.TrackTrace("Consumer::Failed to initialize EventHub", telemetry.Error, map[string]string{"Error": err.Error()})
	}

	// Start consuming messages
	//	consumeMessages()
	consumeMessage()

	// Keep the service running
	for {
		time.Sleep(time.Second)
	}
}
