FROM golang:1.19 AS builder

# Install required packages for Confluent Kafka
RUN apt-get update && apt-get install -y \
    build-essential \
    bash \
    git \
    openssh-client \
    pkg-config \
    librdkafka-dev

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN go build -o chat-service .

FROM golang:1.19
RUN apt-get update && apt-get install -y \
    librdkafka1 \
    ca-certificates

WORKDIR /app
COPY --from=builder /app/chat-service .
COPY .env .
EXPOSE 8080
CMD ["./chat-service"]