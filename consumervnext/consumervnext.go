package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/microtest/telemetry"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
)

const (
	SERVICE_NAME  = "Consumer"
	MaxPartitions = 4
)

func main() {
	// Initialize telemetry
	telemetry.InitTelemetry(SERVICE_NAME)

	eventHubConnectionString := os.Getenv("EVENTHUB_CONSUMERVNEXT_CONNECTION_STRING")
	//	eventHubName := os.Getenv("EVENTHUB_NAME")
	//	partitionID := os.Getenv("EVENTHUB_PARTITION_ID")
	eventHubName := "microtest-eventhub2"
	partitionID := "0"

	fmt.Println("Consumervnext::EventHubName::", eventHubName)
	fmt.Println("Consumervnext::PartitionID::", partitionID)
	fmt.Println("Consumervnext::EventHubConnectionString::", eventHubConnectionString)

	consumerClient, err := createClients(eventHubConnectionString, eventHubName)

	if err != nil {
		fmt.Println("Error creating consumer client::", err)
		panic(err)
	}

	defer consumerClient.Close(context.TODO())

	// Create the Processor
	//
	// The Processor handles load balancing with other Processor instances, running in separate
	// processes or even on separate machines. Each one will use the checkpointStore to coordinate
	// state and ownership, dynamically.
	fmt.Println("Creating processor")
	processor, err := azeventhubs.NewProcessor(consumerClient, nil, nil)

	if err != nil {
		fmt.Println("Error creating processor::", err)
		panic(err)
	}

	// Run in the background, launching goroutines to process each partition
	go dispatchPartitionClients(processor)

	// Run the load balancer. The dispatchPartitionClients goroutine (launched above)
	// will receive and dispatch ProcessorPartitionClients as partitions are claimed.
	//
	// Stopping the processor is as simple as canceling the context that you passed
	// in to Run.
	processorCtx, processorCancel := context.WithCancel(context.TODO())
	defer processorCancel()

	if err := processor.Run(processorCtx); err != nil {
		fmt.Println("Error", err)
		panic(err)
	}
}

func createClients(eventHubConnectionString, eventHubName string) (*azeventhubs.ConsumerClient, error) {

	consumerClient, err := azeventhubs.NewConsumerClientFromConnectionString(eventHubConnectionString, eventHubName, azeventhubs.DefaultConsumerGroup, nil)

	if err != nil {
		fmt.Println("Error creating consumer client::", err)
		return nil, err
	}

	return consumerClient, nil
}

func dispatchPartitionClients(processor *azeventhubs.Processor) {
	for {
		processorPartitionClient := processor.NextPartitionClient(context.TODO())

		if processorPartitionClient == nil {
			// Processor has stopped
			break
		}

		go func() {
			if err := processEventsForPartition(processorPartitionClient); err != nil {
				fmt.Println("Error processing events for partition::", err)
				panic(err)
			}
		}()
	}
}

// processEventsForPartition shows the typical pattern for processing a partition.
func processEventsForPartition(partitionClient *azeventhubs.ProcessorPartitionClient) error {
	// 1. [BEGIN] Initialize any partition specific resources for your application.
	// 2. [CONTINUOUS] Loop, calling ReceiveEvents() and UpdateCheckpoint().
	// 3. [END] Cleanup any resources.

	defer func() {
		// 3/3 [END] Do cleanup here, like shutting down database clients
		// or other resources used for processing this partition.
		shutdownPartitionResources(partitionClient)
	}()

	// 1/3 [BEGIN] Initialize any partition specific resources for your application.
	if err := initializePartitionResources(partitionClient.PartitionID()); err != nil {
		return err
	}

	// 2/3 [CONTINUOUS] Receive events, checkpointing as needed using UpdateCheckpoint.
	for {
		// Wait up to a minute for 100 events, otherwise returns whatever we collected during that time.
		receiveCtx, cancelReceive := context.WithTimeout(context.TODO(), time.Minute)
		events, err := partitionClient.ReceiveEvents(receiveCtx, 100, nil)
		cancelReceive()

		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			var eventHubError *azeventhubs.Error

			if errors.As(err, &eventHubError) && eventHubError.Code == azeventhubs.ErrorCodeOwnershipLost {
				return nil
			}

			return err
		}

		if len(events) == 0 {
			continue
		}

		fmt.Printf("Received %d event(s)\n", len(events))

		for _, event := range events {
			fmt.Printf("Event received with body %v\n", event.Body)
		}

		// Updates the checkpoint with the latest event received. If processing needs to restart
		// it will restart from this point, automatically.
		if err := partitionClient.UpdateCheckpoint(context.TODO(), events[len(events)-1], nil); err != nil {
			return err
		}
	}
}

func shutdownPartitionResources(partitionClient *azeventhubs.ProcessorPartitionClient) {
	// Each PartitionClient holds onto an external resource and should be closed if you're
	// not processing them anymore.
	fmt.Printf("Shutting down resources for partition %s\n", partitionClient.PartitionID())
	defer partitionClient.Close(context.TODO())
}

func initializePartitionResources(partitionID string) error {
	// initialize things that might be partition specific, like a
	// database connection.
	fmt.Printf("Initializing resources for partition %s\n", partitionID)
	return nil
}
