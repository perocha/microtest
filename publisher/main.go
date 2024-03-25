package main

import (
	"encoding/json"
	"net/http"
	"os"
	"time"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/google/uuid"

	"github.com/microtest/common"
)

func publishMessages(w http.ResponseWriter, r *http.Request) {
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

	// Publish messages
	for i := 0; i < message.Count; i++ {
		// Create a new UUID for the message
		messageID := uuid.New().String()

		// Log the event to App Insights
		telemetryData := telemetry.TelemetryData{
			Message: "Publisher::Message received, will publish message to EventHub",
			Properties: map[string]string{
				"messageId": messageID,
				"content":   message.Content,
				"count":     strconv.Itoa(message.Count)},
			Severity: telemetry.Information,
		}
		telemetry.TrackTrace(telemetryData)

		// Publish the message to event hub

	}

	w.WriteHeader(http.StatusOK)
}

func main() {
	// Initialize telemetry
	telemetry.InitTelemetry()

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

	// Log the event to App Insights
	telemetryData := telemetry.TelemetryData{
		Message: "Publisher::ServerStarted on port " + port,
		Properties: map[string]string{"port": port},
		Severity: telemetry.Information,
	}
	telemetry.TrackTrace(telemetryData)

	telemetry.TrackException(server.ListenAndServe())
}
