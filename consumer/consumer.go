package main

import (
	"os"

	"github.com/microtest/messaging"
	"github.com/microtest/storage"
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

	// Create a lease manager for partition leasing using Azure Blob Storage
	storageConnectionString := os.Getenv("STORAGE_CONNECTION_STRING")
	leaseManager, err := storage.NewLeaseManager("Consumer", storageConnectionString, "PartitionLeases")
	if err != nil {
		handleError("Failed to initialize LeaseManager", err)
		return
	}

	// Define the number of partitions and consumer pods
	numPartitions := 4
	numConsumers := 4

	// Define lease duration
	leaseDuration := int32(30) // Adjust lease duration as needed

	// Start consumer pods
	for i := 0; i < numConsumers; i++ {
		go func(consumerID int) {
			// Get a lease for an available partition
			partitionID, err := leaseManager.AcquireLease(consumerID, numPartitions, leaseDuration)
			if err != nil {
				handleError("Failed to acquire lease", err)
				return
			}

			// Start consuming messages from the assigned partition
			consumeMessages(partitionID)
		}(i)
	}

	// Keep the main goroutine running to allow consumer pods to run in the background
	select {}
}

// consumeMessages subscribes to the event hub and consumes messages
func consumeMessages(partitionID string) error {
	// Create a channel to receive messages
	messages := make(chan messaging.Message)

	// Subscribe to messages on the specified partition
	err := messaging.EventHubInstance.ListenForMessages("Consumer", partitionID, messages)
	if err != nil {
		handleError("Failed to subscribe to EventHub", err)
		return err
	}

	// Process received messages
	for msg := range messages {
		// Execute your business logic for each message
		processMessage(msg)
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
	//	telemetry.TrackTrace("Consumer::"+message, telemetry.Error, map[string]string{"Error": err.Error()})
	telemetry.TrackException(err, telemetry.Error, map[string]string{"Error": err.Error(), "Message": message})
}
