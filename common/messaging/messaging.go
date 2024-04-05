package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/microtest/common/telemetry"
)

// EventHubInstance is a global instance of the EventHub
// var producerClient *azeventhubs.ProducerClient
type ProducerClient struct {
	innerClient *azeventhubs.ProducerClient
}

// Message represents the structure of a message
type Message struct {
	Payload   string `json:"payload"`
	MessageId string `json:"messageId"`
}

// NewEventHub initializes a new EventHub instance
func Initialize(serviceName, connectionString, eventHubName string) (*ProducerClient, error) {
	startTime := time.Now()

	// Create a new EventHub instance
	innerClient, err := azeventhubs.NewProducerClientFromConnectionString(connectionString, eventHubName, nil)
	if err != nil {
		telemetry.TrackException(err, telemetry.Error, map[string]string{"Error": err.Error(), "Message": "Failed to create new event hub instance"})
		return nil, err
	}

	// Log the dependency to App Insights (success)
	telemetry.TrackDependency("New event hub initialized", serviceName, "EventHub", eventHubName, true, startTime, time.Now(), nil, "")

	return &ProducerClient{
		innerClient: innerClient,
	}, nil
}

// PublishBatch sends a batch of messages to the EventHub
func (pc *ProducerClient) Publish(ctx context.Context, serviceName string, operationID string, msg Message) error {
	startTime := time.Now()

	// Check if the EventHub instance is initialized, if not return an error
	if pc == nil {
		err := errors.New("eventHub instance not initialized")
		telemetry.TrackException(err, telemetry.Error, map[string]string{"Message": "PublishBatch::Failed to initialize EventHub instance", "Error": err.Error()})
		return err
	}

	// Get the EventHub properties
	eventHubProps, err := pc.innerClient.GetEventHubProperties(context.TODO(), nil)
	if err != nil {
		telemetry.TrackException(err, telemetry.Error, map[string]string{"Message": "PublishBatch::Failed to get EventHub properties", "Error": err.Error()})
		return err
	}
	eventHubName := eventHubProps.Name
	log.Printf("Publish::EventHubName: %s\n", eventHubName)
	for _, partitionID := range eventHubProps.PartitionIDs {
		log.Printf("Publish::PartitionID: %s\n", partitionID)
	}

	// Create a new batch
	batch, err := pc.innerClient.NewEventDataBatch(context.TODO(), nil)
	if err != nil {
		panic(err)
	}

	// Convert the message to JSON
	jsonData, err := json.Marshal(msg)
	if err != nil {
		// Failed to marshal message, log dependency failure to App Insights
		log.Printf("Publish::Failed to marshal message: %s\n", err.Error())
		telemetry.TrackDependency("Publish::Failed to marshal message", serviceName, "EventHub", eventHubName, false, startTime, time.Now(), map[string]string{"Error": err.Error()}, operationID)
		return err
	}

	// can be called multiple times with new messages until you
	// receive an azeventhubs.ErrMessageTooLarge
	err = batch.AddEventData(&azeventhubs.EventData{
		Body: []byte(jsonData),
	}, nil)

	if errors.Is(err, azeventhubs.ErrEventDataTooLarge) {
		// Message too large to fit into this batch.
		//
		// At this point you'd usually just send the batch (using ProducerClient.SendEventDataBatch),
		// create a new one, and start filling up the batch again.
		//
		// If this is the _only_ message being added to the batch then it's too big in general, and
		// will need to be split or shrunk to fit.
		log.Printf("Publish::Message too large to fit into this batch\n")
		panic(err)
	} else if err != nil {
		// Some other error occurred
		log.Printf("Publish::Failed to add message to batch: %s\n", err.Error())
		panic(err)
	}

	// Send the batch
	log.Printf("Publish::Sending batch\n")
	err = pc.innerClient.SendEventDataBatch(context.TODO(), batch, nil)

	if err != nil {
		log.Printf("Publish::Failed to send batch: %s\n", err.Error())
		panic(err)
	}

	log.Printf("Publish::Successfully sent batch\n")
	telemetry.TrackDependencyCtx(ctx, "PublishBatch::Successfully sent batch", serviceName, "EventHub", eventHubName, true, startTime, time.Now(), nil)
	return nil
}
