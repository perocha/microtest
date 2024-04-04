package config

import (
	"context"
	"errors"
	"log"
	"os"

	"github.com/microtest/common/telemetry"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azappconfig"
)

var client *azappconfig.Client

// Initialize the App Configuration client
func InitializeConfig() error {
	connectionString := os.Getenv("APPCONFIGURATION_CONNECTION_STRING")
	if connectionString == "" {
		log.Println("Error: APPCONFIGURATION_CONNECTION_STRING environment variable is not set")
		err := errors.New("app configuration environment variable is not set")
		telemetry.TrackException(err, telemetry.Error, map[string]string{"Error": err.Error(), "Message": "APPCONFIGURATION_CONNECTION_STRING environment variable is not set"})
		return err
	}

	var err error
	client, err = azappconfig.NewClientFromConnectionString(connectionString, nil)
	if err != nil {
		log.Println("Error: Failed to create new App Configuration client")
		telemetry.TrackException(err, telemetry.Error, map[string]string{"Error": err.Error(), "Message": "Failed to create new App Configuration client"})
		return err
	}

	return nil
}

// GetVar retrieves a configuration setting by key
func GetVar(key string) (string, error) {
	if client == nil {
		err := errors.New("app configuration client not initialized")
		telemetry.TrackException(err, telemetry.Error, map[string]string{"Error": err.Error(), "Message": "Failed to create new event hub instance"})
		return "", err
	}

	// Get the setting value from App Configuration
	resp, err := client.GetSetting(context.TODO(), key, nil)
	if err != nil {
		log.Printf("Error: Failed to get configuration setting %s\n", key)
		telemetry.TrackException(err, telemetry.Error, map[string]string{"Error": err.Error(), "Message": "Failed to get configuration setting"})
		return "", err
	}

	return *resp.Value, nil
}
