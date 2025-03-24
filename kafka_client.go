package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	TopicMessages      = "chat.messages"
	TopicEvents        = "chat.events"
	TopicNotifications = "chat.notifications"
	TopicBatchInserts  = "chat.batch.inserts"
	ConsumerGroupID    = "chat-service-group"
)

type KafkaClient struct {
	bootstrapServers string
	apiKey           string
	apiSecret        string
	producer         *kafka.Producer
	consumers        []*kafka.Consumer
	ctx              context.Context
	cancelFunc       context.CancelFunc
	mu               sync.RWMutex
}

func NewKafkaClient(bootstrapServers string) (*KafkaClient, error) {
	ctx, cancel := context.WithCancel(context.Background())
	apiKey := os.Getenv("KAFKA_API_KEY")
	apiSecret := os.Getenv("KAFKA_API_SECRET")

	config := &kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"security.protocol": "SASL_SSL",
		"sasl.mechanisms":   "PLAIN",
		"sasl.username":     apiKey,
		"sasl.password":     apiSecret,
		"acks":              "all",
	}

	producer, err := kafka.NewProducer(config)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create producer: %v", err)
	}

	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("Delivery failed: %v", ev.TopicPartition.Error)
				}
			case kafka.Error:
				log.Printf("Producer error: %v", ev)
			}
		}
	}()

	client := &KafkaClient{
		bootstrapServers: bootstrapServers,
		apiKey:           apiKey,
		apiSecret:        apiSecret,
		producer:         producer,
		ctx:              ctx,
		cancelFunc:       cancel,
	}

	if err := client.checkKafkaConnection(); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to connect to Kafka: %v", err)
	}

	log.Println("Successfully connected to Kafka")
	return client, nil
}

func (k *KafkaClient) checkKafkaConnection() error {
	admin, err := kafka.NewAdminClientFromProducer(k.producer)
	if err != nil {
		return fmt.Errorf("failed to create admin client: %v", err)
	}
	defer admin.Close()

	metadata, err := admin.GetMetadata(nil, true, 5000)
	if err != nil {
		return fmt.Errorf("failed to get metadata: %v", err)
	}
	log.Printf("Connected to Kafka, broker count: %d", len(metadata.Brokers))
	return nil
}

func (k *KafkaClient) PublishMessage(msg MessagePacket) error {
	value, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}
	topic := TopicMessages
	return k.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(msg.ID),
		Value:          value,
		Headers:        []kafka.Header{{Key: "message_type", Value: []byte(msg.Type)}},
	}, nil)
}

func (k *KafkaClient) PublishEvent(event ChatEvent) error {
	value, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %v", err)
	}
	topic := TopicEvents
	return k.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(event.ID),
		Value:          value,
		Headers:        []kafka.Header{{Key: "event_type", Value: []byte(event.Type)}},
	}, nil)
}

func (k *KafkaClient) CreateConsumerGroup(topics []string, groupIDPrefix string) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if groupIDPrefix == "" {
		groupIDPrefix = ConsumerGroupID
	}
	instanceID := os.Getenv("SERVICE_INSTANCE_ID")
	if instanceID == "" {
		instanceID = NewUUID()
	}
	groupID := fmt.Sprintf("%s-%s", groupIDPrefix, instanceID)

	config := &kafka.ConfigMap{
		"bootstrap.servers":  k.bootstrapServers,
		"security.protocol":  "SASL_SSL",
		"sasl.mechanisms":    "PLAIN",
		"sasl.username":      k.apiKey,
		"sasl.password":      k.apiSecret,
		"group.id":           groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "true",
	}

	for _, topic := range topics {
		consumer, err := kafka.NewConsumer(config)
		if err != nil {
			return fmt.Errorf("failed to create consumer for %s: %v", topic, err)
		}
		if err := consumer.Subscribe(topic, nil); err != nil {
			consumer.Close()
			return fmt.Errorf("failed to subscribe to %s: %v", topic, err)
		}
		k.consumers = append(k.consumers, consumer)
		log.Printf("Created consumer for topic %s with group ID %s", topic, groupID)
	}
	return nil
}

func (k *KafkaClient) StartConsumers(server *Server) error {
	if err := k.CreateConsumerGroup([]string{TopicMessages}, ""); err != nil {
		return err
	}
	if err := k.CreateConsumerGroup([]string{TopicEvents}, "events-"); err != nil {
		return err
	}

	k.mu.RLock()
	defer k.mu.RUnlock()
	for _, consumer := range k.consumers {
		topics, err := consumer.Subscription()
		if err != nil {
			log.Printf("Error getting subscription for consumer: %v", err)
			continue
		}
		if len(topics) > 0 {
			switch topics[0] {
			case TopicMessages:
				go k.consumeMessages(consumer, server)
			case TopicEvents:
				go k.consumeEvents(consumer, server)
			}
		}
	}
	log.Println("Kafka consumers started successfully")
	return nil
}

func (k *KafkaClient) consumeMessages(consumer *kafka.Consumer, server *Server) {
	defer consumer.Close()
	for {
		select {
		case <-k.ctx.Done():
			log.Println("Consumer stopped due to context cancellation")
			return
		default:
			msg, err := consumer.ReadMessage(1 * time.Second)
			if err != nil {
				if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
					continue
				}
				log.Printf("Error reading message: %v", err)
				time.Sleep(5 * time.Second)
				continue
			}
			var msgPacket MessagePacket
			if err := json.Unmarshal(msg.Value, &msgPacket); err != nil {
				log.Printf("Error unmarshaling message: %v", err)
				continue
			}
			switch msgPacket.Type {
			case "direct":
				server.routeDirectMessage(msgPacket)
			case "group":
				server.routeGroupMessage(msgPacket)
			}
		}
	}
}

func (k *KafkaClient) consumeEvents(consumer *kafka.Consumer, server *Server) {
	defer consumer.Close()
	for {
		select {
		case <-k.ctx.Done():
			log.Println("Event consumer stopped due to context cancellation")
			return
		default:
			msg, err := consumer.ReadMessage(1 * time.Second)
			if err != nil {
				if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
					continue
				}
				log.Printf("Error reading event: %v", err)
				time.Sleep(5 * time.Second)
				continue
			}
			var event ChatEvent
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				log.Printf("Error unmarshaling event: %v", err)
				continue
			}
			switch event.Type {
			case "message_ack":
				var ack MessageAck
				if err := json.Unmarshal(event.Data, &ack); err != nil {
					log.Printf("Error unmarshaling ack: %v", err)
					continue
				}
				server.processAcknowledgment(ack.MessageID, ack.UserID, ack.Status)
			case "connection":
				server.updateUserPresenceState(event.UserID, true)
			case "disconnection":
				server.updateUserPresenceState(event.UserID, false)
			case "group_created":
				var group GroupMetadata
				if err := json.Unmarshal(event.Data, &group); err != nil {
					log.Printf("Error unmarshaling group data: %v", err)
					continue
				}
				for _, memberID := range group.Members {
					server.connectionsMux.RLock()
					member, ok := server.connections[memberID]
					server.connectionsMux.RUnlock()
					if ok && member.IsConnected {
						if !contains(member.Groups, group.ID) {
							server.connectionsMux.Lock()
							member.Groups = append(member.Groups, group.ID)
							server.connectionsMux.Unlock()
							if memberID != group.CreatedBy {
								notification := CreateNotification("group_added", map[string]interface{}{
									"group_id": group.ID,
									"name":     group.Name,
									"members":  group.Members,
								})
								notificationJSON, _ := json.Marshal(notification)
								member.Send <- notificationJSON
							}
						}
					}
				}
			}
		}
	}
}

func (k *KafkaClient) Close() error {
	k.cancelFunc()
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.producer != nil {
		k.producer.Flush(10000)
		k.producer.Close()
	}
	for _, consumer := range k.consumers {
		consumer.Close()
	}
	return nil
}
