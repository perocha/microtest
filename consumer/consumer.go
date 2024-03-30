package main

import (
	"fmt"
	"os"

	"github.com/microtest/messaging"
	"github.com/microtest/telemetry"
)

const SERVICE_NAME = "Consumer"

func main() {
	// Initialize telemetry
	telemetry.InitTelemetry(SERVICE_NAME)

	// Initialize a new EventHub instance
	eventHubConnectionString := os.Getenv("EVENTHUB_CONSUMER_CONNECTION_STRING")
	err := messaging.NewEventHub("Consumer initialization", eventHubConnectionString)
	if err != nil {
		handleError("Failed to initialize EventHub", err)
		return
	}

	fmt.Println("Consumer initialized")

	// Create a lease manager for partition leasing
	accountName := os.Getenv("STORAGE_ACCOUNT_NAME")
	storageConnectionString := os.Getenv("STORAGE_CONNECTION_STRING")
	partitionLeaseContainer := os.Getenv("PARTITION_LEASE_CONTAINER")

	fmt.Println("Print env variables")
	fmt.Println(accountName)
	fmt.Println(storageConnectionString)
	fmt.Println(partitionLeaseContainer)

	leaseManager, err := messaging.NewLeaseManager(accountName, storageConnectionString, partitionLeaseContainer)

	fmt.Println("After creating lease manager")

	if err != nil {
		fmt.Println("Failed to initialize LeaseManager 1::", err)
		handleError("Failed to initialize LeaseManager", err)
		fmt.Println("Failed to initialize LeaseManager 2::", err)
		return
	}

	// Define the number of partitions and consumer pods
	numPartitions := 4
	numConsumers := 4

	fmt.Println("After defining partitions and consumers")

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
	fmt.Println("handleError::Error: ", err)
	telemetry.TrackException(err, telemetry.Error, map[string]string{"Client": SERVICE_NAME, "Error": err.Error(), "Message": message})
}
