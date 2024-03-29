package main

import (
	"os"

	"github.com/microtest/messaging"
	"github.com/microtest/telemetry"
)

func main() {
	// Initialize telemetry
	telemetry.InitTelemetry("Consumer")

	// Initialize a new EventHub instance
	eventHubConnectionString := os.Getenv("EVENTHUB_CONSUMER_CONNECTION_STRING")
	err := messaging.NewEventHub("Consumer init", eventHubConnectionString)
	if err != nil {
		handleError("Failed to initialize EventHub", err)
		return
	}

	// Start consuming messages
	partitionID := "0"
	err = consumeMessages(partitionID)
	if err != nil {
		handleError("Failed to consume messages", err)
		return
	}
}

// consumeMessages subscribes to the event hub and consumes messages
func consumeMessages(partitionID string) error {
	messages := make(chan messaging.Message)
	bufferSize := 100 // Set an appropriate buffer size

	// Separate goroutine for message processing
	go func() {
		// Create a buffer to temporarily store messages
		messageBuffer := make([]messaging.Message, 0, bufferSize)

		for msg := range messages {
			// Add the message to the buffer
			messageBuffer = append(messageBuffer, msg)

			// Check if the buffer is full
			if len(messageBuffer) >= bufferSize {
				// Process the buffered messages
				for _, bufferedMsg := range messageBuffer {
					processMessage(bufferedMsg)
				}
				// Clear the buffer
				messageBuffer = nil
			}
		}

		// Process any remaining messages in the buffer
		for _, bufferedMsg := range messageBuffer {
			processMessage(bufferedMsg)
		}
	}()

	// Subscribe to messages on the specified partition
	err := messaging.EventHubInstance.ListenForMessages("Consumer", partitionID, messages)
	if err != nil {
		handleError("Failed to subscribe to EventHub", err)
		return err
	}

	return nil
}

// processMessage is the business logic that processes the message
func processMessage(msg messaging.Message) {
	// Log the event to App Insights
	telemetry.TrackTrace("Consumer::processMessage::"+msg.MessageId, telemetry.Information, map[string]string{"payload": msg.Payload, "messageId": msg.MessageId})
}

// handleError logs the error message and error to App Insights
func handleError(message string, err error) {
	// Log the error using telemetry
	telemetry.TrackTrace("Consumer::"+message, telemetry.Error, map[string]string{"Error": err.Error()})
}
