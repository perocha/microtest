package telemetry

import (
	"log"
	"os"

	"github.com/microsoft/ApplicationInsights-Go/appinsights"
	"github.com/microsoft/ApplicationInsights-Go/appinsights/contracts"
)

var client appinsights.TelemetryClient

type TelemetryData struct {
    Message     string
    Severity    contracts.SeverityLevel
    Properties  map[string]string
    Error       error
}

func InitTelemetry() {
	// Get the instrumentation key from environment variables
	instrumentationKey := os.Getenv("APPINSIGHTS_INSTRUMENTATIONKEY")

	if instrumentationKey == "" {
		log.Println("APPINSIGHTS_INSTRUMENTATIONKEY is not set, logging to console instead of App Insights")
		return
	}

	// Create the client
	client = appinsights.NewTelemetryClient(instrumentationKey)

	// Send a trace message to make sure it's working
	client.TrackTrace("App Insights initialized", contracts.Information)
}

// TrackTrace sends a trace message to App Insights
func TrackTrace(message string, severity contracts.SeverityLevel) {
	if client == nil {
		log.Printf("Trace: %s\n", message)
		return
	}

	client.TrackTrace(message, severity)
}


// TrackEvent sends an event to App Insights
func TrackEvent(name string, properties map[string]string, measurements map[string]float64) {
	if client == nil {
		log.Printf("Event: %s, Properties: %v, Measurements: %v\n", name, properties, measurements)
		return
	}
	
	event := appinsights.NewEventTelemetry(name)
	for k, v := range properties {
		event.Properties[k] = v
	}
	for k, v := range measurements {
		event.Measurements[k] = v
	}
	client.Track(event)
}

// TrackException sends an exception to App Insights
func TrackException(err error) {
	if client == nil {
		log.Printf("Exception: %s\n", err.Error())
		return
	}
	
	client.TrackException(err)
}

func CoolTrace(data TelemetryData) {
    if client == nil {
        log.Printf("Telemetry: %s, Properties: %v, Measurements: %v\n", data.Message, data.Properties, data.Measurements)
        return
    }

    trace := appinsights.NewTraceTelemetry(message, severity)
    for k, v := range properties {
        trace.Properties[k] = v
    }
    client.Track(trace)
}
