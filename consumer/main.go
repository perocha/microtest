package main

import (
	"context"
	"os"

	"github.com/microtest/telemetry"
	eventhub "github.com/Azure/azure-event-hubs-go"
)

func main() {
	// Get the connection string from environment variables
	connectionString := os.Getenv("EVENTHUB_CONNECTION_STRING")

	hub, err := eventhub.NewHubFromConnectionString(connectionString)
	if err != nil {
		telemetry.TrackException(err)
		// log.Fatalf("failed to create hub: %s\n", err)
	}

	// Close the hub when the program exits
	defer hub.Close(context.Background())

	// Register a handler for incoming messages
	handler := func(ctx context.Context, event *eventhub.Event) error {
		// Received message, log the event to App Insights
		telemetryData := telemetry.TelemetryData{
			Message: "Received message: " + string(event.Data),
			Properties: map[string]string{"event.Data": string(event.Data)},
			Severity: telemetry.Information,
		}
		telemetry.TrackTrace(telemetryData)

		//fmt.Printf("Received message: %s\n", string(event.Data))
		return nil
	}

	// Start receiving messages
	_, err = hub.Receive(
		context.Background(), 
		"<partition-id>", // replace with your partition ID or use a partition receiver
		handler,
		eventhub.ReceiveWithLatestOffset(), // this is an example of a ReceiveOption you can use
	)
	if err != nil {
		// log.Fatalf("failed to start receiving: %s\n", err)
		telemetry.TrackException(err)
	}

	// Wait indefinitely
	select {}
}
