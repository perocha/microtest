package telemetry

import (
	"log"
	"os"
	"time"

	appinsights "github.com/microsoft/ApplicationInsights-Go/appinsights"
	"github.com/microsoft/ApplicationInsights-Go/appinsights/contracts"
)

var client appinsights.TelemetryClient

type RequestTelemetryData = appinsights.RequestTelemetry

type RemoteDependencyTelemetryData = appinsights.RemoteDependencyTelemetry

// TelemetryData is a struct to hold telemetry data
type TelemetryData struct {
	Message           string
	Id                string // Id to correlate telemetry data
	Severity          contracts.SeverityLevel
	Properties        map[string]string
	Error             error
	DependencyName    string // Name of the command that initiated this dependency call
	DependencyType    string
	DependencyTarget  string
	DependencySuccess bool
	StartTime         time.Time
	EndTime           time.Time
}

// Telemetry severity levels
const (
	Verbose     = contracts.Verbose
	Information = contracts.Information
	Warning     = contracts.Warning
	Error       = contracts.Error
	Critical    = contracts.Critical
)

// InitTelemetry initializes App Insights
func InitTelemetry(serviceName string) {
	// Get the instrumentation key from environment variables
	instrumentationKey := os.Getenv("APPINSIGHTS_INSTRUMENTATIONKEY")

	if instrumentationKey == "" {
		log.Println("APPINSIGHTS_INSTRUMENTATIONKEY is not set, logging to console instead of App Insights")
		return
	}

	// Create the client
	client = appinsights.NewTelemetryClient(instrumentationKey)

	// Set the role name
	client.Context().Tags.Cloud().SetRole(serviceName)

	// Send a trace message to make sure it's working
	client.TrackTrace("Telemetry::App Insights initialized by "+serviceName, contracts.Information)
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
func TrackTrace(Message string, Severity contracts.SeverityLevel, Properties map[string]string) {
	if client == nil {
		log.Printf("Message: %s, Properties: %v, Severity: %v\n", Message, Properties, Severity)
		return
	}

	trace := appinsights.NewTraceTelemetry(Message, Severity)
	for k, v := range Properties {
		trace.Properties[k] = v
	}
	client.Track(trace)
}

// Send a request trace to App Insights
func TrackRequest(data RequestTelemetryData) {
	if client == nil {
		log.Printf("Name: %s, Url: %v, Duration: %s, ResponseCode: %s, Success: %t\n", data.Name, data.Url, data.Duration, data.ResponseCode, data.Success)
		return
	}

	request := appinsights.NewRequestTelemetry(data.Name, data.Url, data.Duration, data.ResponseCode)
	request.Success = data.Success
	for k, v := range data.Properties {
		request.Properties[k] = v
	}
	client.Track(request)
}

// Track a dependency to App Insights
func TrackDependency(
	dependencyData string,
	dependencyName string,
	dependencyType string,
	dependencyTarget string,
	dependencySuccess bool,
	startTime time.Time,
	endTime time.Time,
	properties map[string]string,
) {
	dependency := appinsights.NewRemoteDependencyTelemetry(dependencyName, dependencyType, dependencyTarget, dependencySuccess)
	dependency.Data = dependencyData
	dependency.MarkTime(startTime, endTime)
	for k, v := range properties {
		dependency.Properties[k] = v
	}
	client.Track(dependency)
}
