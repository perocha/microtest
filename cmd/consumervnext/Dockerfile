# Start from the official Go image
FROM golang:1.20-alpine AS builder

# Set necessary environment variables
ENV GO111MODULE=on \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

# Set the Current Working Directory inside the container
WORKDIR /app

# Copy go mod and sum files from the root directory
COPY go.mod go.sum ./

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN go mod download

# Copy the source code and the common package into the container
COPY ./cmd/consumervnext .
COPY ./common/messaging ./common/messaging
COPY ./common/telemetry ./common/telemetry
COPY ./common/config ./common/config
COPY ./common/shared ./common/shared

# Build the Go app
RUN go build -o consumervnext .

# Start a new stage from scratch
FROM alpine:latest  

# Copy the Pre-built binary file from the previous stage
COPY --from=builder /app/consumervnext /

# Command to run the executable
CMD ["/consumervnext"]
