package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/microtest/messaging"
	"github.com/microtest/telemetry"
)

const (
	SERVICE_NAME   = "Consumer"
	leaseDuration  = 30 * time.Second
	NUM_PARTITIONS = 4
	maxRetries     = 5
	backoffDelay   = 20 * time.Second
)

func main() {
	// Initialize telemetry
	telemetry.InitTelemetry(SERVICE_NAME)

	// Initialize a new EventHub instance
	eventHubConnectionString := os.Getenv("EVENTHUB_CONSUMER_CONNECTION_STRING")
	err := messaging.NewEventHub("Consumer initialization", eventHubConnectionString)
	if err != nil {
		handleError("Consumer::Failed to initialize EventHub", err)
		return
	}

	fmt.Println("Consumer::Consumer initialized")

	// Create a lease manager for partition leasing
	accountName := os.Getenv("STORAGE_ACCOUNT_NAME")
	storageConnectionString := os.Getenv("STORAGE_CONNECTION_STRING")
	partitionLeaseContainer := os.Getenv("PARTITION_LEASE_CONTAINER")
	leaseManager, err := messaging.NewLeaseManager(accountName, storageConnectionString, partitionLeaseContainer)

	if err != nil {
		handleError("Consumer::Failed to initialize LeaseManager", err)
		return
	}

	// Try to acquire a lease from each partition
	for i := 0; i < NUM_PARTITIONS; i++ {
		retries := 0
		var partitionID string

		for retries < maxRetries {
			var err error
			partitionID, err = messaging.AcquireLease(leaseManager, NUM_PARTITIONS, int32(leaseDuration.Seconds()))

			if err == nil {
				break
			}

			// Log the error and retry after a delay
			telemetry.TrackTrace("Consumer::AcquireLease::Retrying after delay", telemetry.Error, map[string]string{"retries": strconv.Itoa(retries)})
			time.Sleep(CustomIncrementalDelay(backoffDelay, retries))
			retries++
		}

		if retries == maxRetries {
			// Reached max retries, log error and return
			err := fmt.Errorf("Consumer::Failed to acquire lease after max retries")
			handleError("Consumer::Failed to acquire lease after max retries", err)
			return
		}

		fmt.Println("Consumer::Acquired lease for partition:", partitionID)

		// Start consuming messages from the assigned partition
		consumeMessages(partitionID)
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
		handleError("Consumer::Failed to subscribe to EventHub", err)
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
	fmt.Println("Consumer::handleError::Message: ", message)
	fmt.Println("Consumer::handleError::Error: ", err)
	telemetry.TrackException(err, telemetry.Error, map[string]string{"Client": SERVICE_NAME, "Error": err.Error(), "Message": message})
}

// CustomIncrementalDelay generates an incremental delay based on the current delay and retry count.
func CustomIncrementalDelay(currentDelay time.Duration, retry int) time.Duration {
	incrementFactor := 10
	return currentDelay * time.Duration(incrementFactor)
}
