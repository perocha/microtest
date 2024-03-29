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
	err := messaging.EventHubInstance.Subscribe("Consumer", func(msg messaging.Message) {
		// Log the event to App Insights
		telemetry.TrackTrace("Consumer::Message received from EventHub", telemetry.Information, map[string]string{"payload": msg.Payload, "messageId": msg.MessageId})
	})

	// Check if there was an error subscribing to the event hub
	if err != nil {
		telemetry.TrackTrace("Consumer::Failed to subscribe to EventHub", telemetry.Error, map[string]string{"Error": err.Error()})
	}
}

func main() {
	// Initialize telemetry
	telemetry.InitTelemetry("Consumer")

	// Initialize a new EventHub instance
	eventHubConnectionString := os.Getenv("EVENTHUB_CONSUMER_CONNECTION_STRING")
	err := messaging.NewEventHub("Consumer", eventHubConnectionString)
	if err != nil {
		telemetry.TrackTrace("Consumer::Failed to initialize EventHub", telemetry.Error, map[string]string{"Error": err.Error()})
	}

	// Start consuming messages
	consumeMessages()

	// Keep the service running
	for {
		time.Sleep(time.Second)
	}
}
