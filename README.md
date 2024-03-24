# microtest
Experiments with microservices


# Repository Structure

publisher
A simple service that publishes messages to a queue. It has a single endpoint that accepts a POST request with a message in the body. The message is then published to a EventHubs queue.

consumer
A simple service that consumes messages from a EventHubs queue. When a message is received, it is logged to the console.

common
This folder contains shared code that is used by both the publisher and consumer services.
* telemetry - Contains code for logging telemetry data to App Insights

# Configuration

## Environment Variables

For now, App Insights is the only service that requires an environment variable to be set.
* APPINSIGHTS_INSTRUMENTATIONKEY


