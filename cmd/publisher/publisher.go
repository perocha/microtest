package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"

	"github.com/microtest/common/config"
	"github.com/microtest/common/messaging"
	"github.com/microtest/common/shared"
	"github.com/microtest/common/telemetry"
)

const (
	SERVICE_NAME = "Publisher"
)

// Messaging client to publish messages to the event hub
var producer *messaging.ProducerClient

func main() {
	err := initializeApp()
	if err != nil {
		log.Println("Publisher::Error initializing app", err)
		panic(err)
	}

	// Start the HTTP server
	startHTTPServer()

	// Graceful shutdown
	// defer func() {
	// 	// Close the EventHub client
	// 	err := producer.Close()
}

func initializeApp() error {
	// Get the configuration settings from App Configuration
	err := config.InitializeConfig()
	if err != nil {
		log.Println("Consumervnext::Error initializing config", err)
		panic(err)
	}
	appinsights_instrumentationkey, _ := config.GetVar("APPINSIGHTS_INSTRUMENTATIONKEY")
	eventHubName, _ := config.GetVar("EVENTHUB_NAME")
	eventHubConnectionString, _ := config.GetVar("EVENTHUB_PUBLISHER_CONNECTION_STRING")
	log.Println("Publisher::AppInsightsInstrumentationKey::", appinsights_instrumentationkey)
	log.Println("Publisher::EventHubName::", eventHubName)
	log.Println("Publisher::EventHubConnectionString::", eventHubConnectionString)

	// Initialize telemetry
	err = telemetry.InitTelemetryKey(SERVICE_NAME, appinsights_instrumentationkey)
	if err != nil {
		log.Println("Consumervnext::Error initializing telemetry", err)
		panic(err)
	}

	// Initialize a new EventHub instance
	producerInstance, err := messaging.ProducerInit(SERVICE_NAME, eventHubConnectionString, eventHubName)
	if err != nil {
		// Failed to initialize EventHub, log the error to App Insights
		telemetry.TrackException(err, telemetry.Error, map[string]string{"Message": "Publisher::Failed to initialize EventHub", "Error": err.Error()})
		panic(err)
	}

	// Set the global producer instance
	telemetry.TrackTrace("Publisher::Initialization complete", telemetry.Information, map[string]string{"EventHubName": eventHubName}, "")
	producer = producerInstance

	return nil
}

// Initialize HTTP server and routes
func startHTTPServer() {
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
	telemetry.TrackTrace("Publisher::ServerStarted on port "+port, telemetry.Information, map[string]string{"port": port}, "")

	// Start the server
	err := server.ListenAndServe()
	if err != nil {
		// Failed to start server, log the error to App Insights
		telemetry.TrackException(err, telemetry.Error, map[string]string{"Message": "Publisher::Failed to start server", "Error": err.Error()})
		panic(err)
	}
}

// Publishes messages to the event hub
func publishMessages(w http.ResponseWriter, r *http.Request) {
	// Start time for tracking duration
	startTime := time.Now()

	// Parse request body into the Event struct
	var event messaging.Event
	err := json.NewDecoder(r.Body).Decode(&event)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Get a new operation ID to track the end-to-end request and add it to the context
	operationID := telemetry.TrackRequest(r.URL.Path, r.URL.String(), time.Since(startTime), strconv.Itoa(http.StatusOK), true, r.RemoteAddr, nil)
	ctx := context.WithValue(context.Background(), shared.OperationIDKeyContextKey, operationID)

	// Generate a unique UUID for the event
	event.EventID = uuid.New().String()

	// Add the current timestamp to the event
	event.Timestamp = time.Now()

	// Publish the message to event hub
	err = producer.PublishMessage(ctx, SERVICE_NAME, operationID, event)

	if err != nil {
		// Failed to publish message, log the error to App Insights
		telemetry.TrackTraceCtx(ctx, "Publisher::Failed to publish message: "+event.EventID+")", telemetry.Error, map[string]string{"Error": err.Error()})
	}

	// Send HTTP response with status code 200 (OK)
	w.WriteHeader(http.StatusOK)
}
