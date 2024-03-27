package main

import (
	"os"
	"time"
	"log"

	"github.com/microtest/telemetry"
	"github.com/microtest/messaging"
)

// Method consumeMessages subscribes to the event hub and consumes messages
func consumeMessages() {
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
	eventHubConnectionString := os.Getenv("EVENT_HUB_CONNECTION_STRING")
	err := messaging.NewEventHub("Consumer", eventHubConnectionString)
	if err != nil {
		log.Println("consumer::failed to initialize EventHub: " + err.Error())

		telemetryData := telemetry.TelemetryData{
			Message: "Consumer::Failed to initialize EventHub",
			Properties: map[string]string{"Error": err.Error()},
			Severity: telemetry.Error,
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
