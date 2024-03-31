package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/microtest/telemetry"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
)

const (
	SERVICE_NAME = "Consumer"
)

// Shows how to start consuming events in partitions in an Event Hub using the [ConsumerClient].
//
// If you have an Azure Storage account you can use the [Processor] type instead, which will handle
// distributing partitions between multiple consumers and storing progress using checkpoints.
// See [example_consuming_with_checkpoints_test.go] for an example.
//
// [example_consuming_with_checkpoints_test.go]: https://github.com/Azure/azure-sdk-for-go/blob/main/sdk/messaging/azeventhubs/example_consuming_with_checkpoints_test.go
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

	// Create new consumer client using connection string
	consumerClient, err := azeventhubs.NewConsumerClientFromConnectionString(eventHubConnectionString, eventHubName, azeventhubs.DefaultConsumerGroup, nil)

	if err != nil {
		telemetry.TrackException(err, telemetry.Error, map[string]string{"Error": err.Error(), "Message": "Failed to create new event hub instance"})
		fmt.Println("Consumervnext::Failed to create new consumer client::" + err.Error())
		panic(err)
	}

	defer consumerClient.Close(context.TODO())

	partitionClient, err := consumerClient.NewPartitionClient(partitionID, &azeventhubs.PartitionClientOptions{
		StartPosition: azeventhubs.StartPosition{
			Earliest: to.Ptr(true),
		},
	})

	if err != nil {
		telemetry.TrackException(err, telemetry.Error, map[string]string{"Error": err.Error(), "Message": "Failed to create new partition client"})
		fmt.Println("Consumervnext::Failed to create new partition client::" + err.Error())
		panic(err)
	}

	defer partitionClient.Close(context.TODO())

	// Will wait up to 1 minute for 100 events. If the context is cancelled (or expires)
	// you'll get any events that have been collected up to that point.
	fmt.Println("Receiving events...")
	receiveCtx, cancel := context.WithTimeout(context.TODO(), time.Minute)
	events, err := partitionClient.ReceiveEvents(receiveCtx, 100, nil)
	cancel()

	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		telemetry.TrackException(err, telemetry.Error, map[string]string{"Error": err.Error(), "Message": "Error receiving events"})
		fmt.Println("Consumervnext::Error receiving events::", err)
		panic(err)
	}

	for _, event := range events {
		// We're assuming the Body is a byte-encoded string. EventData.Body supports any payload
		// that can be encoded to []byte.
		telemetry.TrackTrace("Event received with body", telemetry.Information, map[string]string{"body": string(event.Body)})
		fmt.Println("Consumervnext::Event received with body::", string(event.Body))
	}

	telemetry.TrackTrace("Done receiving events", telemetry.Information, nil)
	fmt.Println("Consumervnext::Done receiving events")

}
