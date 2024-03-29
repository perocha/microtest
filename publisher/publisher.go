package main

import (
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"

	"github.com/microtest/messaging"
	"github.com/microtest/telemetry"
)

// Method publishMessages publishes messages to the event hub
func publishMessages(w http.ResponseWriter, r *http.Request) {
	// Start time for tracking duration
	startTime := time.Now()

	// Parse request body
	type Message struct {
		Content string `json:"content"`
		Count   int    `json:"count"`
	}
	var message Message
	err := json.NewDecoder(r.Body).Decode(&message)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Publish X number of messages, based on the count received in the POST request
	for i := 0; i < message.Count; i++ {
		// Create a unique UUID for each message sent to event hub
		messageID := uuid.New().String()

		// Create a new message to be sent to the event hub (with payload received in POST and the unique message id)
		msg := messaging.Message{
			Payload:   message.Content,
			MessageId: messageID,
		}

		// Log the event to App Insights
		telemetry.TrackTrace("Publisher::Message received, will publish message to EventHub (messageID: "+messageID+")",
			telemetry.Information, map[string]string{"messageId": messageID, "content": message.Content, "count": strconv.Itoa(message.Count)})

		// Publish the message to event hub
		err = messaging.EventHubInstance.Publish("Publisher", msg)

		if err != nil {
			// Failed to publish message, log the error to App Insights
			telemetry.TrackTrace("Publisher::Failed to publish message: "+messageID+")",
				telemetry.Error, map[string]string{"Error": err.Error()})
		}
	}

	// Everything was OK, track the "request" call trace to App Insights
	telemetry.TrackRequest(r.URL.Path, r.URL.String(), time.Since(startTime), strconv.Itoa(http.StatusOK), true, r.RemoteAddr, nil)

	// Send HTTP response with status code 200 (OK)
	w.WriteHeader(http.StatusOK)
}

func main() {
	// Initialize telemetry
	telemetry.InitTelemetry("Publisher")

	// Initialize a new EventHub instance
	eventHubConnectionString := os.Getenv("EVENTHUB_PUBLISHER_CONNECTION_STRING")
	err := messaging.NewEventHub("Publisher", eventHubConnectionString)
	if err != nil {
		// Failed to initialize EventHub, log the error to App Insights
		telemetry.TrackTrace("Publisher::Failed to initialize EventHub",
			telemetry.Error, map[string]string{"Error": err.Error()})
	}

	// Create a new router
	router := mux.NewRouter()

	// Define REST API endpoint for publishing messages
	router.HandleFunc("/publish", publishMessages).Methods("POST")

	// Start HTTP server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080" // Default port if not specified
	}
	server := &http.Server{
		Addr:         ":" + port,
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// Server started in the specified port, log to App Insights
	telemetry.TrackTrace("Publisher::ServerStarted on port "+port,
		telemetry.Information, map[string]string{"port": port})

	// Start the server
	telemetry.TrackException(server.ListenAndServe())
}
