package main

import (
	"os"

	"github.com/microtest/telemetry"
	"github.com/microtest/messaging"
)

func consumeMessages() {
	// Subscribe to the event hub
	err := messaging.EventHubInstance.Subscribe(func(message string) {
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
	telemetry.InitTelemetry()

	// Initialize a new EventHub instance
	eventHubConnectionString := os.Getenv("EVENT_HUB_CONNECTION_STRING")
	err := messaging.NewEventHub(eventHubConnectionString)
	if err != nil {
		telemetryData := telemetry.TelemetryData{
			Message: "Consumer::Failed to initialize EventHub",
			Properties: map[string]string{"Error": err.Error()},
			Severity: telemetry.Error,
		}
		telemetry.TrackTrace(telemetryData)
	}

	// Start consuming messages
	consumeMessages()
}
