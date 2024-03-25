# microtest
Experiments with microservices using simple services written in golang. GitHub actions will be used to build and deploy the microservices in Azure Kubernetes Service. The services will use App Insights for observability.

# Code structure

publisher
A service that publishes messages to a queue. It has an endpoint that accepts a POST request (defaul port 8080) with a message in the body and a count that determines how many times the message will be sent. The message (or messages) is then published to a EventHubs topic.

consumer
A service that consumes messages from a topic in EventHubs.

common
This folder contains shared code that is used by both the publisher and consumer services.
* telemetry - logging telemetry data to App Insights
* messaging (TODO) - code to handle all interaction with EventHubs (pubsub)

# Build & Deployment

Build and deployment are done using Github Actions.

First the image is compiled and pushed to Azure Container Registry. Each service has its own Dockerfile to create the microservice image.

Once the image is built and published in ACR, the workflow will deploy the image in Azure AKS cluster.

In folder k8s you'll find the Kubernetes deployment files:
* publisher-deployment.yaml - manages the deployment of publisher microservice
* publisher-service.yaml - manages the deployment of a Loadbalancer that will open port 80 and redirect requests to PODs running service publisher (round robin load balancing).

# Cross cutting features

In folder "common" there are several cross-cutting features that each microservice can use. This includes telemetry, messaging and will be expanded based on the needs.

## Telemetry

The telemetry is handled by package telemetry.go. I'm using App Insights to trace all microservice actions.

## Messaging

// TODO The messaging is handled by package messaging.go

## Configuration

### Environment Variables

For now, the configuration is managed using environment variables:
* telemetry: APPINSIGHTS_INSTRUMENTATIONKEY - App Insights key
* publisher: PORT - Port that will be listening to requests
* messaging: EVENT_HUB_NAME - Event Hubs name
* publisher: EVENT_HUB_CONNECTION_STRING - Event Hubs connection string

// TODO: move this configuration to App Config service in Azure. How to dynamically configure PORT in publisher-service?

