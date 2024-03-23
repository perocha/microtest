package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"
	"strconv"

	"github.com/gorilla/mux"

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
		// Log the event to App Insights
		common.TrackEvent("MessagePublished", map[string]string{
			"messageId": strconv.Itoa(i),
			"content": message.Content,
			"count": strconv.Itoa(message.Count),
			}, nil)

		// Publish the message to event hub

	}

	w.WriteHeader(http.StatusOK)
}

func main() {
	// Initialize telemetry
	common.InitTelemetry()

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
	common.TrackEvent("ServerStarted", map[string]string{"port": port}, nil)

	log.Fatal(server.ListenAndServe())
}
