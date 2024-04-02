package telemetry

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/microtest/shared"

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
	client.TrackTrace(serviceName+"::Telemetry::App Insights initialized", contracts.Information)
}

// TrackException sends an exception to App Insights
func TrackException(err error, Severity contracts.SeverityLevel, Properties map[string]string) {
	if client == nil {
		log.Printf("Exception: %s\n", err.Error())
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
func TrackTrace(Message string, Severity contracts.SeverityLevel, Properties map[string]string, parentID string) string {
	if client == nil {
		log.Printf("Message: %s, Properties: %v, Severity: %v\n", Message, Properties, Severity)
		return ""
	}

	trace := appinsights.NewTraceTelemetry(Message, Severity)
	for k, v := range Properties {
		trace.Properties[k] = v
	}

	// Set parent id
	if parentID != "" {
		trace.Tags.Operation().SetParentId(parentID)
	}
	client.Track(trace)

	// Return the operation id
	return trace.Tags.Operation().GetId()
}

// Sends a trace message to App Insights
func TrackTraceCtx(ctx context.Context, Message string, Severity contracts.SeverityLevel, Properties map[string]string) string {
	if client == nil {
		log.Printf("Message: %s, Properties: %v, Severity: %v\n", Message, Properties, Severity)
		return ""
	}

	// Create the new trace
	trace := appinsights.NewTraceTelemetry(Message, Severity)
	for k, v := range Properties {
		trace.Properties[k] = v
	}

	// Get the operationID from the context
	if operationID, ok := ctx.Value(shared.OperationIDKeyContextKey).(string); ok {
		// Set parent id
		if operationID != "" {
			trace.Tags.Operation().SetParentId(operationID)
		}
	}

	// Send the trace to App Insights
	client.Track(trace)

	// Return the operation id
	return trace.Tags.Operation().GetId()
}

// Send a request trace to App Insights
func TrackRequest(Method, Url string, Duration time.Duration, ResponseCode string, Success bool, Source string, Properties map[string]string) string {
	if client == nil {
		log.Printf("Name: %s, Url: %v, Duration: %s, ResponseCode: %s, Success: %t\n", Method, Url, Duration, ResponseCode, Success)
		return ""
	}

	request := appinsights.NewRequestTelemetry(Method, Url, Duration, ResponseCode)
	request.Success = Success
	for k, v := range Properties {
		request.Properties[k] = v
	}

	// Send the request to App Insights
	client.Track(request)

	// Return the operation id
	return request.Tags.Operation().GetId()
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
	parentID string,
) string {
	// Create more descriptive information to trace, with the caller name and the dependency data
	dependencyText := dependencyName + "::" + dependencyData

	dependency := appinsights.NewRemoteDependencyTelemetry(dependencyText, dependencyType, dependencyTarget, dependencySuccess)

	dependency.Data = dependencyData
	dependency.MarkTime(startTime, endTime)
	for k, v := range properties {
		dependency.Properties[k] = v
	}

	// Set parent id
	if parentID != "" {
		dependency.Tags.Operation().SetParentId(parentID)
	}

	client.Track(dependency)

	return dependency.Tags.Operation().GetId()
}

// Track a dependency to App Insights
func TrackDependencyCtx(
	ctx context.Context,
	dependencyData string,
	dependencyName string,
	dependencyType string,
	dependencyTarget string,
	dependencySuccess bool,
	startTime time.Time,
	endTime time.Time,
	properties map[string]string,
) string {
	// Create more descriptive information to trace, with the caller name and the dependency data
	dependencyText := dependencyName + "::" + dependencyData

	dependency := appinsights.NewRemoteDependencyTelemetry(dependencyText, dependencyType, dependencyTarget, dependencySuccess)

	dependency.Data = dependencyData
	dependency.MarkTime(startTime, endTime)
	for k, v := range properties {
		dependency.Properties[k] = v
	}

	// Get the operationID from the context
	if operationID, ok := ctx.Value(shared.OperationIDKeyContextKey).(string); ok {
		// Set parent id
		if operationID != "" {
			dependency.Tags.Operation().SetParentId(operationID)
		}
	}

	// Send the dependency to App Insights
	client.Track(dependency)

	return dependency.Tags.Operation().GetId()
}
