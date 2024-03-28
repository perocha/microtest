package main

import (
	"context"
	"fmt"
	"log"
	"os"

	eventhub "github.com/Azure/azure-event-hubs-go"
)

/*
import (
	"log"
	"os"
	"time"

	"github.com/microtest/messaging"
	"github.com/microtest/telemetry"

	eventhub "github.com/Azure/azure-event-hubs-go"
)

// Method consumeMessages subscribes to the event hub and consumes messages
func consumeMessages() {
	log.Println("consumer::subscribing to EventHub")

	// Subscribe to the event hub
	err := messaging.EventHubInstance.Subscribe(func(message string) {
		// Log to console (not app insights)
		log.Println("consumer::message received: " + message)

		// Log the event to App Insights
		telemetryData := telemetry.TelemetryData{
			Message: "Consumer::Message received from EventHub",
			Properties: map[string]string{
				"content": message,
			},
			Severity: telemetry.Information,
		}
		telemetry.TrackTrace(telemetryData)
	})

	if err != nil {
		// Failed to subscribe to the event hub
		log.Println("consumer::failed to subscribe to EventHub: " + err.Error())

		telemetryData := telemetry.TelemetryData{
			Message: "Consumer::Failed to subscribe to EventHub",
			Properties: map[string]string{
				"Error": err.Error(),
			},
			Severity: telemetry.Error,
		}
		telemetry.TrackTrace(telemetryData)
	}
}

func main() {
	// Initialize telemetry
	telemetry.InitTelemetry("Consumer")
	log.Println("consumer::initialized telemetry")

	// Initialize a new EventHub instance
	eventHubConnectionString := os.Getenv("EVENTHUB_CONSUMER_CONNECTION_STRING")
	err := messaging.NewEventHub("Consumer", eventHubConnectionString)
	if err != nil {
		log.Println("consumer::failed to initialize EventHub: " + err.Error())

		telemetryData := telemetry.TelemetryData{
			Message:    "Consumer::Failed to initialize EventHub",
			Properties: map[string]string{"Error": err.Error()},
			Severity:   telemetry.Error,
		}
		telemetry.TrackTrace(telemetryData)
	}

	// Start consuming messages
	consumeMessages()

	// Keep the service running
	for {
		time.Sleep(time.Second)
	}
}
*/


func main() {
	// Replace with your connection string
	connStr := os.Getenv("EVENTHUB_CONSUMER_CONNECTION_STRING")

	hub, err := eventhub.NewHubFromConnectionString(connStr)
	if err != nil {
		log.Fatalf("failed to get hub: %s\n", err)
	}

	// Register a handler for the Event Hub
	_, err = hub.Receive(
		context.Background(),
		"0", // This is the partition ID
		func(ctx context.Context, event *eventhub.Event) error {
			fmt.Printf("received: %s\n", string(event.Data))
			return nil
		},
		eventhub.ReceiveWithLatestOffset(),
	)

	if err != nil {
		log.Fatalf("failed to receive: %s\n", err)
	}

	select {} // Keeps the program running to receive messages
}