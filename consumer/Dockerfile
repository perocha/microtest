# Use the official Golang image to create a build artifact.
FROM golang:1.16 as builder

# Copy local code to the container image.
WORKDIR /app
COPY go.* ./
RUN go mod download
COPY . ./

# Build the command inside the container.
RUN CGO_ENABLED=0 GOOS=linux go build -v -o consumer

# Use a Docker multi-stage build to create a lean production image.
FROM alpine:3
RUN apk add --no-cache ca-certificates

# Copy the binary to the production image from the builder stage.
COPY --from=builder /app/consumer /consumer

# Run the web service on container startup.
CMD ["/consumer"]
