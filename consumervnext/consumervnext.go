package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/microtest/telemetry"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/checkpoints"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

const (
	SERVICE_NAME  = "Consumer"
	MaxPartitions = 4
)

func main() {
	// Initialize telemetry
	telemetry.InitTelemetry(SERVICE_NAME)

	eventHubConnectionString := os.Getenv("EVENTHUB_CONSUMERVNEXT_CONNECTION_STRING")
	checkpointStoreConnectionString := os.Getenv("CHECKPOINTSTORE_STORAGE_CONNECTION_STRING")
	//	eventHubName := os.Getenv("EVENTHUB_NAME")
	//	partitionID := os.Getenv("EVENTHUB_PARTITION_ID")
	eventHubName := "microtest-eventhub2"
	partitionID := "0"
	containerName := "partitionlease"

	fmt.Println("Consumervnext::EventHubName::", eventHubName)
	fmt.Println("Consumervnext::PartitionID::", partitionID)
	fmt.Println("Consumervnext::ContainerName::", containerName)
	fmt.Println("Consumervnext::EventHubConnectionString::", eventHubConnectionString)
	fmt.Println("Consumervnext::CheckpointStoreConnectionString::", checkpointStoreConnectionString)

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

	// create a processor to receive and process events
	processor, err := azeventhubs.NewProcessor(consumerClient, checkpointStore, nil)

	if err != nil {
		handleError("Consumervnext::Error creating processor", err)
		panic(err)
	}

	//  for each partition in the event hub, create a partition client with processEvents as the function to process events
	dispatchPartitionClients := func() {
		for {
			partitionClient := processor.NextPartitionClient(context.TODO())

			telemetry.TrackTrace("Consumervnext::Create partition client::PartitionID::"+partitionClient.PartitionID(), telemetry.Information, map[string]string{"Client": SERVICE_NAME, "PartitionID": partitionClient.PartitionID()})

			if partitionClient == nil {
				break
			}

			go func() {
				if err := processEvents(partitionClient); err != nil {
					handleError("Consumervnext::Error processing events", err)
					panic(err)
				}
			}()
		}
	}

	// run all partition clients
	telemetry.TrackTrace("Consumervnext::Starting partition clients", telemetry.Information, map[string]string{"Client": SERVICE_NAME})
	go dispatchPartitionClients()

	processorCtx, processorCancel := context.WithCancel(context.TODO())
	defer processorCancel()

	if err := processor.Run(processorCtx); err != nil {
		handleError("Consumervnext::Error processor run", err)
		panic(err)
	}
}

func processEvents(partitionClient *azeventhubs.ProcessorPartitionClient) error {
	defer closePartitionResources(partitionClient)
	for {
		receiveCtx, receiveCtxCancel := context.WithTimeout(context.TODO(), time.Minute)
		events, err := partitionClient.ReceiveEvents(receiveCtx, 100, nil)
		receiveCtxCancel()

		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}

		//fmt.Printf("Consumervnext::PartitionID::%s::Processing %d event(s)\n", partitionClient.PartitionID(), len(events))

		for _, event := range events {
			fmt.Printf("Consumervnext::PartitionID::%s::Events received %v\n", partitionClient.PartitionID(), string(event.Body))
			fmt.Printf("Offset: %d Sequence number: %d MessageID: %v PartitionKey: %v\n", event.Offset, event.SequenceNumber, event.MessageID, event.PartitionKey)
			telemetry.TrackTrace("Consumervnext::PartitionID::"+partitionClient.PartitionID()+"::Event received", telemetry.Information, map[string]string{"Client": SERVICE_NAME, "PartitionID": partitionClient.PartitionID(), "Event": string(event.Body)})
		}

		if len(events) != 0 {
			if err := partitionClient.UpdateCheckpoint(context.TODO(), events[len(events)-1], nil); err != nil {
				telemetry.TrackTrace("Consumervnext::PartitionID::"+partitionClient.PartitionID()+"::Error updating checkpoint", telemetry.Error, map[string]string{"Client": SERVICE_NAME, "PartitionID": partitionClient.PartitionID(), "Error": err.Error()})
				return err
			}
		}
	}
}

func closePartitionResources(partitionClient *azeventhubs.ProcessorPartitionClient) {
	defer partitionClient.Close(context.TODO())
}

// handleError logs the error message and error to App Insights
func handleError(message string, err error) {
	// Log the error using telemetry
	fmt.Println("Consumer::handleError::Message: ", message)
	fmt.Println("Consumer::handleError::Error: ", err)
	telemetry.TrackException(err, telemetry.Error, map[string]string{"Client": SERVICE_NAME, "Error": err.Error(), "Message": message})
}
