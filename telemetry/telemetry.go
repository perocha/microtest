package telemetry

import (
	"log"
	"os"
	"time"

	"github.com/google/uuid"
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
}

// Sends a trace message to App Insights
func TrackTrace(Message string, Severity contracts.SeverityLevel, Properties map[string]string) {
	// Create a unique UUID for this operation
	operationID := uuid.New().String()

	if client == nil {
		log.Println("Message: %s, Properties: %v, Severity: %v", Message, Properties, Severity)
		return
	}

	trace := appinsights.NewTraceTelemetry(Message, Severity)
	trace.Properties["OperationID"] = operationID
	log.Printf("TrackTrace::Troubleshoot:$%s$\n", operationID)
	for k, v := range Properties {
		trace.Properties[k] = v
	}
	client.Track(trace)
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
	// Create a unique UUID for this operation
	operationID := uuid.New().String()

	// Create more descriptive information to trace, with the caller name and the dependency data
	dependencyText := dependencyName + "::" + dependencyData

	dependency := appinsights.NewRemoteDependencyTelemetry(dependencyText, dependencyType, dependencyTarget, dependencySuccess)

	dependency.Data = dependencyData
	dependency.MarkTime(startTime, endTime)
	for k, v := range properties {
		dependency.Properties[k] = v
	}
	dependency.Id = operationID
	test1 := client.Context().CommonProperties["OperationID"]
	test2 := client.Context().Tags.Device().GetId()
	test3 := client.Context().Tags.Session().GetId()
	test4 := client.Context().Tags.Operation().GetId()
	test5 := client.Context().Tags.Operation().GetParentId()

	client.Track(dependency)

	log.Printf("TrackDependency::test1:$%s$\n", test1)
	log.Printf("TrackDependency::test2:$%s$\n", test2)
	log.Printf("TrackDependency::test3:$%s$\n", test3)
	log.Printf("TrackDependency::test4:$%s$\n", test4)
	log.Printf("TrackDependency::test5:$%s$\n", test5)
}
