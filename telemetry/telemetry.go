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
func TrackException(err error, Severity contracts.SeverityLevel, Properties map[string]string) {
	if client == nil {
		log.Println("Exception: %s", err.Error())
		return
	}

	exception := appinsights.NewExceptionTelemetry(err)
	exception.SeverityLevel = Severity
	for k, v := range Properties {
		exception.Properties[k] = v
	}

	client.Track(exception)

	operationID := client.Context().Tags.Operation().GetId()
	log.Println("TrackException::OperationID: %s", operationID)
}

// Sends a trace message to App Insights
func TrackTrace(Message string, Severity contracts.SeverityLevel, Properties map[string]string) {
	if client == nil {
		log.Println("Message: %s, Properties: %v, Severity: %v", Message, Properties, Severity)
		return
	}

	trace := appinsights.NewTraceTelemetry(Message, Severity)
	for k, v := range Properties {
		trace.Properties[k] = v
	}
	client.Track(trace)

	operationID := trace.Properties["Operation Id"]
	log.Println("TrackTrace::OperationID: %s", operationID)
}

// Send a request trace to App Insights
func TrackRequest(Method string, Url string, Duration time.Duration, ResponseCode string, Success bool, Source string, Properties map[string]string) {
	if client == nil {
		log.Println("Name: %s, Url: %v, Duration: %s, ResponseCode: %s, Success: %t", Method, Url, Duration, ResponseCode, Success)
		return
	}

	request := appinsights.NewRequestTelemetry(Method, Url, Duration, ResponseCode)
	request.Success = Success
	for k, v := range Properties {
		request.Properties[k] = v
	}
	client.Track(request)

	operationID := client.Context().Tags.Operation().GetId()
	log.Println("TrackRequest::OperationID: %s", operationID)
	log.Println("TrackRequest::Id: ", request.Id)
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
	// Create more descriptive information to trace, with the caller name and the dependency data
	dependencyText := dependencyName + "::" + dependencyData

	dependency := appinsights.NewRemoteDependencyTelemetry(dependencyText, dependencyType, dependencyTarget, dependencySuccess)
	dependency.Data = dependencyData
	dependency.MarkTime(startTime, endTime)
	for k, v := range properties {
		dependency.Properties[k] = v
	}
	client.Track(dependency)

	operationID := client.Context().Tags.Operation().GetId()
	log.Println("TrackDependency::OperationID: %s\n", operationID)
	log.Println("TrackDependency::Id: ", dependency.Id)
}
