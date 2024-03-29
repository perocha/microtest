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
		telemetry.TrackTraceTest("Consumer::Message received from EventHub", telemetry.Information, map[string]string{"payload": msg.Payload, "messageId": msg.MessageId})

		/*
			telemetryData := telemetry.TelemetryData{
				Message: "Consumer::Message received from EventHub",
				Properties: map[string]string{
					"payload":   msg.Payload,
					"messageId": msg.MessageId,
				},
				Severity: telemetry.Information,
			}
			telemetry.TrackTrace(telemetryData)
		*/
	})

	// Check if there was an error subscribing to the event hub
	if err != nil {
		telemetry.TrackTraceTest("Consumer::Failed to subscribe to EventHub", telemetry.Error, map[string]string{"Error": err.Error()})

		/*
			telemetryData := telemetry.TelemetryData{
				Message: "Consumer::Failed to subscribe to EventHub",
				Properties: map[string]string{
					"Error": err.Error(),
				},
				Severity: telemetry.Error,
			}
			telemetry.TrackTrace(telemetryData)
		*/
	}
}

func main() {
	// Initialize telemetry
	telemetry.InitTelemetry("Consumer")

	// Initialize a new EventHub instance
	eventHubConnectionString := os.Getenv("EVENTHUB_CONSUMER_CONNECTION_STRING")
	err := messaging.NewEventHub("Consumer", eventHubConnectionString)
	if err != nil {
		telemetry.TrackTraceTest("Consumer::Failed to initialize EventHub", telemetry.Error, map[string]string{"Error": err.Error()})
		/*
			telemetryData := telemetry.TelemetryData{
				Message: "Consumer::Failed to initialize EventHub",
				Properties: map[string]string{
					"Error": err.Error(),
				},
				Severity: telemetry.Error,
			}
			telemetry.TrackTrace(telemetryData)
		*/
	}

	// Start consuming messages
	consumeMessages()

	// Keep the service running
	for {
		time.Sleep(time.Second)
	}
}
