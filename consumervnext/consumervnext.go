package main

import (
	"context"
	"errors"
	"log"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/microtest/telemetry"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/checkpoints"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

const (
	SERVICE_NAME  = "Consumervnext"
	MaxPartitions = 4
)

func main() {
	// Initialize telemetry
	telemetry.InitTelemetry(SERVICE_NAME)

	// Get the connection strings from environment variables
	eventHubConnectionString := os.Getenv("EVENTHUB_CONSUMERVNEXT_CONNECTION_STRING")
	checkpointStoreConnectionString := os.Getenv("CHECKPOINTSTORE_STORAGE_CONNECTION_STRING")
	//	eventHubName := os.Getenv("EVENTHUB_NAME")
	eventHubName := "microtest-eventhub2"
	containerName := "partitionlease"

	log.Println("Consumervnext::EventHubName::", eventHubName)
	log.Println("Consumervnext::EventHubName::", eventHubName)
	log.Println("Consumervnext::ContainerName::", containerName)
	log.Println("Consumervnext::EventHubConnectionString::", eventHubConnectionString)
	log.Println("Consumervnext::CheckpointStoreConnectionString::", checkpointStoreConnectionString)

	// create a container client using a connection string and container name
	checkClient, err := container.NewClientFromConnectionString(checkpointStoreConnectionString, containerName, nil)

	if err != nil {
		handleError("Consumervnext::Error creating container client", err)
		panic(err)
	}

	// create a checkpoint store that will be used by the event hub
	checkpointStore, err := checkpoints.NewBlobStore(checkClient, nil)

	if err != nil {
		handleError("Consumervnext::Error creating checkpoint store", err)
		panic(err)
	}

	// create a consumer client using a connection string to the namespace and the event hub
	consumerClient, err := azeventhubs.NewConsumerClientFromConnectionString(eventHubConnectionString, eventHubName, azeventhubs.DefaultConsumerGroup, nil)

	if err != nil {
		handleError("Consumervnext::Error creating consumer client", err)
		panic(err)
	}

	defer consumerClient.Close(context.TODO())

	// Create a processor to receive and process events
	processor, err := azeventhubs.NewProcessor(consumerClient, checkpointStore, nil)

	if err != nil {
		handleError("Consumervnext::Error creating processor", err)
		panic(err)
	}

	// For each partition in the event hub, create a partition client with processEvents as the function to process events
	dispatchPartitionClients := func() {
		for {
			// Track time and create a new operation ID, that will be used to track the end to end operation
			startTime := time.Now()
			operationID := uuid.New().String()

			// Get the next partition client
			partitionClient := processor.NextPartitionClient(context.TODO())

			if partitionClient == nil {
				// No more partition clients to process
				break
			}

			go func() {
				// Create a new context with the operation ID
				ctx := context.WithValue(context.Background(), "operationID", operationID)

				log.Printf("Consumervnext::PartitionID::%s::Partition client initialized\n", partitionClient.PartitionID())
				telemetry.TrackDependency("New partition client initialized for partition "+partitionClient.PartitionID(), SERVICE_NAME, "EventHub", eventHubName, true, startTime, time.Now(), map[string]string{"PartitionID": partitionClient.PartitionID()}, operationID)

				if err := processEvents(ctx, partitionClient); err != nil {
					handleError("Consumervnext::Error processing events for partition "+partitionClient.PartitionID(), err)
					panic(err)
				}
			}()
		}
	}

	// Run all partition clients
	go dispatchPartitionClients()

	processorCtx, processorCancel := context.WithCancel(context.TODO())
	defer processorCancel()

	if err := processor.Run(processorCtx); err != nil {
		handleError("Consumervnext::Error processor run", err)
		panic(err)
	}
}

// ProcessEvents implements the logic that is executed when events are received from the event hub
func processEvents(ctx context.Context, partitionClient *azeventhubs.ProcessorPartitionClient) error {
	defer closePartitionResources(partitionClient)
	for {
		receiveCtx, receiveCtxCancel := context.WithTimeout(ctx, time.Minute)
		events, err := partitionClient.ReceiveEvents(receiveCtx, 100, nil)
		receiveCtxCancel()

		// Get the operation ID from the context
		operationID := ctx.Value("operationID").(string)

		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}

		// Uncomment the following line to verify that the consumer is trying to receive events
		// log.Printf("Consumervnext::PartitionID::%s::Processing %d event(s)\n", partitionClient.PartitionID(), len(events))

		for _, event := range events {
			// Events received!! Process the message
			log.Printf("Consumervnext::PartitionID::%s::Events received %v\n", partitionClient.PartitionID(), string(event.Body))
			log.Printf("Offset: %d Sequence number: %d MessageID: %s\n", event.Offset, event.SequenceNumber, *event.MessageID)
			telemetry.TrackTrace("Consumervnext::PartitionID::"+partitionClient.PartitionID()+"::Event received", telemetry.Information, map[string]string{"Client": SERVICE_NAME, "PartitionID": partitionClient.PartitionID(), "Event": string(event.Body)}, operationID)
		}

		if len(events) != 0 {
			if err := partitionClient.UpdateCheckpoint(context.TODO(), events[len(events)-1], nil); err != nil {
				handleError("Consumervnext::Error updating checkpoint", err)
				return err
			}
		}
	}
}

// Closes the partition client
func closePartitionResources(partitionClient *azeventhubs.ProcessorPartitionClient) {
	defer partitionClient.Close(context.TODO())
}

// Logs the error message and sends an exception to App Insights
func handleError(message string, err error) {
	// Log the error using telemetry
	log.Println("Consumervnext::handleError::Message: ", message)
	log.Println("Consumervnext::handleError::Error: ", err)
	telemetry.TrackException(err, telemetry.Error, map[string]string{"Client": SERVICE_NAME, "Error": err.Error(), "Message": message})
}
