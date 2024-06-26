package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/checkpoints"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/microtest/common/telemetry"
)

type Order struct {
	Id              string
	ProductCategory string
	ProductID       string
	CustomerID      string
	Status          string
}

type Event struct {
	Type         string
	EventID      string
	Timestamp    time.Time
	OrderPayload Order
}

// EventHub producer client
type ProducerClient struct {
	innerClient *azeventhubs.ProducerClient
}

// EventHub consumer client
type Processor struct {
	innerClient *azeventhubs.Processor
}

// Message represents the structure of a message
type Message struct {
	Payload   string `json:"payload"`
	MessageId string `json:"messageId"`
}

// Initialize a new EventHub producer instance
func ProducerInit(serviceName, connectionString, eventHubName string) (*ProducerClient, error) {
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

// Close the EventHub producer instance
func (pc *ProducerClient) ProducerClose() error {
	startTime := time.Now()

	// Check if the EventHub instance is initialized, if not return
	if pc == nil {
		err := errors.New("eventHub instance not initialized")
		telemetry.TrackException(err, telemetry.Error, map[string]string{"Message": "Close::Failed to initialize EventHub instance", "Error": err.Error()})
		return err
	}

	// Close the EventHub instance
	err := pc.innerClient.Close(context.Background())
	if err != nil {
		telemetry.TrackException(err, telemetry.Error, map[string]string{"Message": "Failed to close EventHub instance", "Error": err.Error()})
		return err
	}

	// Log the dependency to App Insights (success)
	telemetry.TrackDependency("Close::EventHub instance closed", "", "EventHub", "", true, startTime, time.Now(), nil, "")

	return nil
}

// Sends a message to the EventHub
func (pc *ProducerClient) PublishMessage(ctx context.Context, serviceName string, operationID string, event Event) error {
	startTime := time.Now()

	// Check if the EventHub instance is initialized, if not return an error
	if pc == nil {
		err := errors.New("eventHub instance not initialized")
		telemetry.TrackException(err, telemetry.Error, map[string]string{"Message": "PublishBatch::Failed to initialize EventHub instance", "Error": err.Error()})
		return err
	}

	// Get the EventHub name
	eventHubProps, err := pc.innerClient.GetEventHubProperties(context.TODO(), nil)
	if err != nil {
		telemetry.TrackException(err, telemetry.Error, map[string]string{"Message": "PublishBatch::Failed to get EventHub properties", "Error": err.Error()})
		return err
	}
	eventHubName := eventHubProps.Name

	// Create a new batch
	batch, err := pc.innerClient.NewEventDataBatch(context.TODO(), nil)
	if err != nil {
		panic(err)
	}

	// Convert the message to JSON
	jsonData, err := json.Marshal(event)
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
	err = pc.innerClient.SendEventDataBatch(context.TODO(), batch, nil)

	if err != nil {
		log.Printf("Publish::Failed to send message with error: %s\n", err.Error())
		telemetry.TrackDependency("Publish::Failed to send message", serviceName, "EventHub", eventHubName, false, startTime, time.Now(), map[string]string{"Error": err.Error()}, operationID)
		panic(err)
	}

	log.Printf("Publish::Successfully sent message with size=%d::content=%s\n", len(jsonData), jsonData)
	telemetry.TrackDependencyCtx(ctx, "PublishBatch::Successfully sent batch", serviceName, "EventHub", eventHubName, true, startTime, time.Now(), nil)
	return nil
}

// Consumer initialization
func ProcessorInit(serviceName, eventHubConnectionString, eventHubName, containerName, checkpointStoreConnectionString string) (*Processor, error) {
	startTime := time.Now()

	// Create a container client using a connection string and container name
	checkClient, err := container.NewClientFromConnectionString(checkpointStoreConnectionString, containerName, nil)
	if err != nil {
		telemetry.TrackException(err, telemetry.Error, map[string]string{"Client": serviceName, "Error": err.Error(), "Message": "Error creating container client"})
		panic(err)
	}

	// Create a checkpoint store that will be used by the event hub
	checkpointStore, err := checkpoints.NewBlobStore(checkClient, nil)
	if err != nil {
		telemetry.TrackException(err, telemetry.Error, map[string]string{"Client": serviceName, "Error": err.Error(), "Message": "Error creating checkpoint store"})
		panic(err)
	}

	// Create a consumer client using a connection string to the namespace and the event hub
	consumerClient, err := azeventhubs.NewConsumerClientFromConnectionString(eventHubConnectionString, eventHubName, azeventhubs.DefaultConsumerGroup, nil)
	if err != nil {
		telemetry.TrackException(err, telemetry.Error, map[string]string{"Client": serviceName, "Error": err.Error(), "Message": "Error creating consumer client"})
		panic(err)
	}

	defer consumerClient.Close(context.TODO())

	// Create a processor to receive and process events
	innerClient, err := azeventhubs.NewProcessor(consumerClient, checkpointStore, nil)
	if err != nil {
		telemetry.TrackException(err, telemetry.Error, map[string]string{"Client": serviceName, "Error": err.Error(), "Message": "Error creating processor"})
		panic(err)
	}

	// Log the dependency to App Insights (success)
	telemetry.TrackDependency("New event hub consumer initialized", serviceName, "EventHub", eventHubName, true, startTime, time.Now(), nil, "")

	return &Processor{
		innerClient: innerClient,
	}, nil
}
