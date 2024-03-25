package telemetry

import (
	"log"
	"os"

	"github.com/microsoft/ApplicationInsights-Go/appinsights"
	"github.com/microsoft/ApplicationInsights-Go/appinsights/contracts"
)

var client appinsights.TelemetryClient

// TelemetryData is a struct to hold telemetry data
type TelemetryData struct {
    Message     string
    Severity    contracts.SeverityLevel
    Properties  map[string]string
    Error       error
}

// Telemetry severity levels
const (
	Verbose     = contracts.Verbose
	Information = contracts.Information
	Warning     = contracts.Warning
	Error       = contracts.Error
	Critical    = contracts.Critical
)

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
	client.TrackTrace("Telemetry::App Insights initialized", contracts.Information)
}

// TrackException sends an exception to App Insights
func TrackException(err error) {
	if client == nil {
		log.Printf("Exception: %s\n", err.Error())
		return
	}

	// TODO: How to write a Message in the Exception??

	client.TrackException(err)
}

// Sends a trace message to App Insights
func TrackTrace(data TelemetryData) {
	if client == nil {
        log.Printf("Telemetry: %s, Properties: %v\n", data.Message, data.Properties)
        return
    }

    trace := appinsights.NewTraceTelemetry(data.Message, data.Severity)
    for k, v := range data.Properties {
        trace.Properties[k] = v
    }
    client.Track(trace)
}
