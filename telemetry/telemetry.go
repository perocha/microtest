package telemetry

import (
	"log"
	"os"
	"time"

	"github.com/microsoft/ApplicationInsights-Go/appinsights"
	"github.com/microsoft/ApplicationInsights-Go/appinsights/contracts"
)

var client appinsights.TelemetryClient

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
func TrackTrace(data TelemetryData) {
	if client == nil {
		log.Printf("Message: %s, Properties: %v\n", data.Message, data.Properties)
		return
	}

	trace := appinsights.NewTraceTelemetry(data.Message, data.Severity)
	for k, v := range data.Properties {
		trace.Properties[k] = v
	}
	client.Track(trace)
}

// Track a dependency to App Insights
func TrackDependencyBase(data TelemetryData) {
	if client == nil {
		log.Printf(("DependencyName: %s, DependencyType: %s, DependencyTarget: %s, DependencySuccess: %t, StartTime: %s, EndTime: %s\n"), data.DependencyName, data.DependencyType, data.DependencyTarget, data.DependencySuccess, data.StartTime, data.EndTime)
		return
	}

	dependency := appinsights.NewRemoteDependencyTelemetry(data.DependencyName, data.DependencyType, data.DependencyTarget, data.DependencySuccess)
	dependency.MarkTime(data.StartTime, data.EndTime)
	for k, v := range data.Properties {
		dependency.Properties[k] = v
	}
	client.Track(dependency)
}

// Helper function to generate a TrackDependency
func TrackDependency(
	dependencyName string,
	dependencyType string,
	dependencyTarget string,
	dependencySuccess bool,
	startTime time.Time,
	endTime time.Time,
	properties map[string]string,
) {

	telemetryData := TelemetryData{
		DependencyName:    dependencyName,
		DependencyType:    dependencyType,
		DependencyTarget:  dependencyTarget,
		DependencySuccess: dependencySuccess,
		StartTime:         startTime,
		EndTime:           endTime,
	}

	TrackDependencyBase(telemetryData)
}
