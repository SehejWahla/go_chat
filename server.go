package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Server struct {
	instanceID        string
	connections       map[string]*Client
	redisClient       *redis.Client
	redisPubSub       *redis.PubSub
	mongoClient       *mongo.Client
	kafkaClient       *KafkaClient
	upgrader          websocket.Upgrader
	ctx               context.Context
	cancelFunc        context.CancelFunc
	connectionsMux    sync.RWMutex
	messageBatch      []interface{}
	messageBatchMutex sync.Mutex
	batchTicker       *time.Ticker
	sequenceNumbers   map[string]int64
	sequenceNumMutex  sync.Mutex
}

func NewServer() (*Server, error) {
	ctx, cancel := context.WithCancel(context.Background())
	instanceID := os.Getenv("SERVICE_INSTANCE_ID")
	if instanceID == "" {
		instanceID = NewUUID()
	}
	return &Server{
		instanceID:  instanceID,
		connections: make(map[string]*Client),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin:     func(r *http.Request) bool { return true },
		},
		ctx:             ctx,
		cancelFunc:      cancel,
		messageBatch:    make([]interface{}, 0, 100),
		sequenceNumbers: make(map[string]int64),
	}, nil
}

func (s *Server) Initialize() error {
	if err := s.connectToRedis(); err != nil {
		return fmt.Errorf("failed to connect to Redis: %v", err)
	}
	if err := s.connectToMongoDB(); err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %v", err)
	}

	bootstrapServers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	if bootstrapServers == "" {
		return fmt.Errorf("KAFKA_BOOTSTRAP_SERVERS not set")
	}
	var err error
	s.kafkaClient, err = NewKafkaClient(bootstrapServers)
	if err != nil {
		return fmt.Errorf("failed to create Kafka client: %v", err)
	}

	if err := s.initRedisSubscriptions(); err != nil {
		return fmt.Errorf("failed to initialize Redis subscriptions: %v", err)
	}

	s.batchTicker = time.NewTicker(5 * time.Second)
	if err := s.registerInstance(); err != nil {
		return fmt.Errorf("failed to register instance: %v", err)
	}

	go s.validateConnectionStatuses()
	log.Printf("Server initialized with instance ID: %s", s.instanceID)
	return nil
}

func (s *Server) connectToRedis() error {
	redisHost := os.Getenv("REDIS_HOST")
	redisPort := os.Getenv("REDIS_PORT")
	redisUsername := os.Getenv("REDIS_USERNAME")
	redisPassword := os.Getenv("REDIS_PASSWORD")

	// Check if required variables are set
	if redisHost == "" || redisPort == "" {
		return fmt.Errorf("REDIS_HOST and REDIS_PORT must be set")
	}

	s.redisClient = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", redisHost, redisPort),
		Username: redisUsername, // Optional, defaults to "" if not set
		Password: redisPassword, // Optional, defaults to "" if not set
	})

	_, err := s.redisClient.Ping(s.ctx).Result()
	if err != nil {
		return fmt.Errorf("failed to connect to Redis: %v", err)
	}
	log.Println("Connected to Redis")
	return nil
}

func (s *Server) connectToMongoDB() error {
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		return fmt.Errorf("MONGO_URI environment variable is not set")
	}
	clientOptions := options.Client().ApplyURI(mongoURI)
	var err error
	s.mongoClient, err = mongo.Connect(s.ctx, clientOptions)
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %v", err)
	}
	if err = s.mongoClient.Ping(s.ctx, nil); err != nil {
		return fmt.Errorf("MongoDB ping failed: %v", err)
	}
	if err := s.createMongoDBIndexes(); err != nil {
		return fmt.Errorf("failed to create MongoDB indexes: %v", err)
	}
	log.Println("Connected to MongoDB")
	return nil
}

func (s *Server) createMongoDBIndexes() error {
	messagesCollection := s.mongoClient.Database("chat").Collection("messages")
	_, err := messagesCollection.Indexes().CreateOne(s.ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "to", Value: 1}, {Key: "timestamp", Value: -1}},
	})
	if err != nil {
		return fmt.Errorf("failed to create message index: %v", err)
	}
	_, err = messagesCollection.Indexes().CreateOne(s.ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "id", Value: 1}, {Key: "status", Value: 1}},
	})
	if err != nil {
		return fmt.Errorf("failed to create message status index: %v", err)
	}
	return nil
}

func (s *Server) initRedisSubscriptions() error {
	instanceChannel := fmt.Sprintf("instance:%s", s.instanceID)
	s.redisPubSub = s.redisClient.Subscribe(s.ctx, instanceChannel, "global:notifications")
	go s.handleRedisMessages()
	log.Printf("Subscribed to Redis channels: %s, global:notifications", instanceChannel)
	return nil
}

func (s *Server) handleRedisMessages() {
	for {
		msg, err := s.redisPubSub.ReceiveMessage(s.ctx)
		if err != nil {
			if s.ctx.Err() != nil {
				return
			}
			log.Printf("Error receiving Redis message: %v", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if msg.Channel == fmt.Sprintf("instance:%s", s.instanceID) {
			var message map[string]interface{}
			if err := json.Unmarshal([]byte(msg.Payload), &message); err != nil {
				log.Printf("Error unmarshaling Redis message: %v", err)
				continue
			}
			switch message["type"] {
			case "forward_message":
				recipientID := message["to"].(string)
				payload := message["payload"].(string)
				var msgPacket MessagePacket
				if err := json.Unmarshal([]byte(payload), &msgPacket); err != nil {
					log.Printf("Error unmarshaling forwarded message: %v", err)
					continue
				}
				s.connectionsMux.RLock()
				recipient, ok := s.connections[recipientID]
				s.connectionsMux.RUnlock()
				if ok && recipient.IsConnected {
					recipient.Send <- []byte(payload)
					s.sendMessageAck(msgPacket.ID, recipientID, msgPacket.From, "delivered")
				} else {
					s.storePendingMessage(msgPacket)
				}
			case "forward_group_message":
				// Existing group message handling
			case "forward_notification":
				toUserID := message["to"].(string)
				payload := message["payload"].(string)
				s.connectionsMux.RLock()
				client, ok := s.connections[toUserID]
				s.connectionsMux.RUnlock()
				if ok && client.IsConnected {
					client.Send <- []byte(payload)
				}
			case "message_ack":
				messageID := message["message_id"].(string)
				userID := message["from"].(string)
				status := message["status"].(string)
				s.processAcknowledgment(messageID, userID, status)
			}
		}
	}
}

func (s *Server) registerInstance() error {
	err := s.redisClient.Set(s.ctx, fmt.Sprintf("instances:%s", s.instanceID), "active", 60*time.Second).Err()
	if err != nil {
		return err
	}
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.redisClient.Set(s.ctx, fmt.Sprintf("instances:%s", s.instanceID), "active", 60*time.Second)
			case <-s.ctx.Done():
				return
			}
		}
	}()
	log.Printf("Registered instance %s", s.instanceID)
	return nil
}

func (s *Server) RegisterRoutes(router *mux.Router) {
	router.HandleFunc("/ws/chat", s.handleWebSocketConnection)
	router.HandleFunc("/health", s.handleHealthCheck)
	router.HandleFunc("/version", s.handleVersion).Methods("GET")
}

func (s *Server) handleWebSocketConnection(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		http.Error(w, "Missing user_id", http.StatusBadRequest)
		return
	}
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Error upgrading connection: %v", err)
		return
	}
	client := NewClient(userID, conn, s)
	s.registerClient(client)
	s.deliverPendingMessages(client)
	s.loadUserGroups(client)

	go client.readPump()
	go client.writePump()
}

func (s *Server) deliverPendingMessages(client *Client) {
	collection := s.mongoClient.Database("chat").Collection("pending_messages")
	filter := bson.M{"to": client.UserID}
	cursor, err := collection.Find(s.ctx, filter, options.Find().SetSort(bson.D{{Key: "timestamp", Value: 1}}))
	if err != nil {
		log.Printf("Error fetching pending messages: %v", err)
		return
	}
	defer cursor.Close(s.ctx)

	var deliveredMsgIDs []string
	for cursor.Next(s.ctx) {
		var message MessagePacket
		if err := cursor.Decode(&message); err != nil {
			log.Printf("Error decoding pending message: %v", err)
			continue
		}
		message.Status = "delivered"
		message.DeliveredAt = time.Now()
		messageJSON, _ := json.Marshal(message)
		client.Send <- messageJSON
		deliveredMsgIDs = append(deliveredMsgIDs, message.ID)
		s.sendMessageAck(message.ID, client.UserID, message.From, "delivered")
	}
	if len(deliveredMsgIDs) > 0 {
		collection.DeleteMany(s.ctx, bson.M{"id": bson.M{"$in": deliveredMsgIDs}})
	}
}

func (s *Server) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":      "ok",
		"instance_id": s.instanceID,
		"connections": len(s.connections),
	})
}

func (s *Server) registerClient(client *Client) {
	s.connectionsMux.Lock()
	defer s.connectionsMux.Unlock()
	if existing, ok := s.connections[client.UserID]; ok {
		existing.Connection.Close()
		delete(s.connections, client.UserID)
	}
	s.connections[client.UserID] = client
	connectionData, _ := json.Marshal(client.connInfo)
	s.redisClient.Set(s.ctx, fmt.Sprintf("conn:%s", client.ConnectionID), connectionData, 24*time.Hour)
	s.redisClient.Set(s.ctx, fmt.Sprintf("user:%s:conn", client.UserID), client.ConnectionID, 24*time.Hour)
	s.redisClient.Set(s.ctx, fmt.Sprintf("user:%s:instance", client.UserID), s.instanceID, 24*time.Hour)
	s.updateUserPresenceState(client.UserID, true)
	log.Printf("Client registered: %s (connection %s)", client.UserID, client.ConnectionID)
}

func (s *Server) unregisterClient(client *Client) {
	s.connectionsMux.Lock()
	defer s.connectionsMux.Unlock()
	if c, ok := s.connections[client.UserID]; ok && c == client {
		delete(s.connections, client.UserID)
		close(client.Send)
	}
	client.IsConnected = false
	client.updateConnectionInfo()
	s.redisClient.Del(s.ctx, fmt.Sprintf("user:%s:instance", client.UserID))
	s.updateUserPresenceState(client.UserID, false)
	log.Printf("Client unregistered: %s", client.UserID)
}

func (s *Server) updateConnectionInfo(connInfo UserConnection) {
	connectionData, _ := json.Marshal(connInfo)
	s.redisClient.Set(s.ctx, fmt.Sprintf("conn:%s", connInfo.ConnectionID), connectionData, 24*time.Hour)
	s.redisClient.Set(s.ctx, fmt.Sprintf("user:%s:last_seen", connInfo.UserID), connInfo.LastSeen.Unix(), 24*time.Hour)
	if connInfo.LastSequence > 0 {
		s.redisClient.Set(s.ctx, fmt.Sprintf("user:%s:last_seq", connInfo.UserID), connInfo.LastSequence, 24*time.Hour)
	}
}

func (s *Server) loadUserGroups(client *Client) {
	collection := s.mongoClient.Database("chat").Collection("user_groups")
	var result struct {
		UserID string   `bson:"user_id"`
		Groups []string `bson:"groups"`
	}
	err := collection.FindOne(s.ctx, bson.M{"user_id": client.UserID}).Decode(&result)
	if err != nil && err != mongo.ErrNoDocuments {
		log.Printf("Error loading user groups: %v", err)
		return
	}
	client.Groups = result.Groups
}

func (s *Server) handleClientDirectMessage(client *Client, toUserID string, content json.RawMessage, clientMsgID string) {
	chatID := getDirectChatID(client.UserID, toUserID)
	seqNum := s.getNextSequenceNumber(chatID)
	message := MessagePacket{
		ID:          NewUUID(),
		Type:        "direct",
		From:        client.UserID,
		To:          toUserID,
		Content:     content,
		Timestamp:   time.Now(),
		Status:      "sent",
		ClientMsgID: clientMsgID,
		SequenceNum: seqNum,
	}
	messageJSON, _ := json.Marshal(message)
	client.Send <- messageJSON
	client.addUnacknowledgedMessage(message)
	s.routeDirectMessage(message)
	s.updateUserSequenceNumber(client.UserID, seqNum)
}

func (s *Server) routeDirectMessage(message MessagePacket) {
	recipientID := message.To
	s.connectionsMux.RLock()
	recipient, ok := s.connections[recipientID]
	s.connectionsMux.RUnlock()
	if ok && recipient.IsConnected {
		messageJSON, _ := json.Marshal(message)
		recipient.Send <- messageJSON
		s.sendMessageAck(message.ID, recipientID, message.From, "delivered")
	} else {
		instanceID, err := s.redisClient.Get(s.ctx, fmt.Sprintf("user:%s:instance", recipientID)).Result()
		if err == nil && instanceID != "" && instanceID != s.instanceID {
			messageJSON, _ := json.Marshal(message)
			forwardCmd, _ := json.Marshal(map[string]interface{}{
				"type":    "forward_message",
				"to":      recipientID,
				"payload": string(messageJSON),
			})
			s.redisClient.Publish(s.ctx, fmt.Sprintf("instance:%s", instanceID), forwardCmd)
		} else {
			s.storePendingMessage(message)
		}
	}
	s.addToBatch("messages", message)
}

func (s *Server) storePendingMessage(message MessagePacket) {
	collection := s.mongoClient.Database("chat").Collection("pending_messages")
	message.Status = "pending"
	_, err := collection.InsertOne(s.ctx, message)
	if err != nil {
		log.Printf("Error storing pending message: %v", err)
	}
}

func (s *Server) sendMessageAck(messageID, fromUserID, toUserID, status string) {
	ack := MessageAck{
		UserID:    fromUserID,
		MessageID: messageID,
		Status:    status,
		Timestamp: time.Now(),
	}
	ackJSON, _ := json.Marshal(ack)
	s.redisClient.Set(s.ctx, fmt.Sprintf("ack:%s:%s", messageID, fromUserID), ackJSON, 24*time.Hour)
	s.addToBatch("acks", map[string]interface{}{
		"message_id": messageID,
		"user_id":    fromUserID,
		"status":     status,
		"timestamp":  time.Now(),
	})

	if toUserID == "" {
		collection := s.mongoClient.Database("chat").Collection("messages")
		var message MessagePacket
		err := collection.FindOne(s.ctx, bson.M{"id": messageID}).Decode(&message)
		if err != nil {
			log.Printf("Error finding message for ack: %v", err)
			return
		}
		toUserID = message.From
	}

	notification := CreateNotification("message_ack", map[string]interface{}{
		"message_id": messageID,
		"user_id":    fromUserID,
		"status":     status,
	})
	notificationJSON, _ := json.Marshal(notification)
	s.connectionsMux.RLock()
	sender, ok := s.connections[toUserID]
	s.connectionsMux.RUnlock()
	if ok && sender.IsConnected {
		sender.removeUnacknowledgedMessage(messageID)
		sender.Send <- notificationJSON
	} else {
		instanceID, err := s.redisClient.Get(s.ctx, fmt.Sprintf("user:%s:instance", toUserID)).Result()
		if err == nil && instanceID != "" && instanceID != s.instanceID {
			forwardCmd, _ := json.Marshal(map[string]interface{}{
				"type":    "forward_notification",
				"to":      toUserID,
				"payload": string(notificationJSON),
			})
			s.redisClient.Publish(s.ctx, fmt.Sprintf("instance:%s", instanceID), forwardCmd)
		}
	}
}

func (s *Server) processAcknowledgment(messageID, fromUserID, status string) {
	notification := CreateNotification("message_ack", map[string]interface{}{
		"message_id": messageID,
		"user_id":    fromUserID,
		"status":     status,
	})
	collection := s.mongoClient.Database("chat").Collection("messages")
	var message MessagePacket
	err := collection.FindOne(s.ctx, bson.M{"id": messageID}).Decode(&message)
	if err != nil {
		log.Printf("Error finding message for ack: %v", err)
		return
	}
	senderID := message.From
	s.connectionsMux.RLock()
	sender, ok := s.connections[senderID]
	s.connectionsMux.RUnlock()
	if ok && sender.IsConnected {
		sender.removeUnacknowledgedMessage(messageID)
		notificationJSON, _ := json.Marshal(notification)
		sender.Send <- notificationJSON
	}
	update := bson.M{"$set": bson.M{"status": status}}
	if status == "delivered" {
		update["$set"].(bson.M)["delivered_at"] = time.Now()
	} else if status == "read" {
		update["$set"].(bson.M)["read_at"] = time.Now()
	}
	collection.UpdateOne(s.ctx, bson.M{"id": messageID}, update)
}

func (s *Server) handleClientGroupMessage(client *Client, groupID string, content json.RawMessage, clientMsgID string) {
	if !contains(client.Groups, groupID) {
		errorNotification := CreateNotification("error", map[string]interface{}{
			"message": "Not a member of this group",
		})
		errorJSON, _ := json.Marshal(errorNotification)
		client.Send <- errorJSON
		return
	}
	seqNum := s.getNextSequenceNumber(groupID)
	message := MessagePacket{
		ID:          NewUUID(),
		Type:        "group",
		From:        client.UserID,
		To:          groupID,
		Content:     content,
		Timestamp:   time.Now(),
		Status:      "sent",
		ClientMsgID: clientMsgID,
		SequenceNum: seqNum,
	}
	messageJSON, _ := json.Marshal(message)
	client.Send <- messageJSON
	s.routeGroupMessage(message)
	s.updateUserSequenceNumber(client.UserID, seqNum)
}

func (s *Server) routeGroupMessage(message MessagePacket) {
	groupID := message.To
	collection := s.mongoClient.Database("chat").Collection("groups")
	var group GroupMetadata
	err := collection.FindOne(s.ctx, bson.M{"_id": groupID}).Decode(&group)
	if err != nil {
		log.Printf("Error fetching group %s: %v", groupID, err)
		return
	}
	membersOnThisInstance := []string{}
	membersByInstance := make(map[string][]string)
	offlineMembers := []string{}
	for _, memberID := range group.Members {
		if memberID == message.From {
			continue
		}
		s.connectionsMux.RLock()
		member, ok := s.connections[memberID]
		s.connectionsMux.RUnlock()
		if ok && member.IsConnected {
			membersOnThisInstance = append(membersOnThisInstance, memberID)
		} else {
			instanceID, err := s.redisClient.Get(s.ctx, fmt.Sprintf("user:%s:instance", memberID)).Result()
			if err == nil && instanceID != "" && instanceID != s.instanceID {
				membersByInstance[instanceID] = append(membersByInstance[instanceID], memberID)
			} else {
				offlineMembers = append(offlineMembers, memberID)
			}
		}
	}
	messageJSON, _ := json.Marshal(message)
	for _, memberID := range membersOnThisInstance {
		s.connectionsMux.RLock()
		member, ok := s.connections[memberID]
		s.connectionsMux.RUnlock()
		if ok && member.IsConnected {
			member.Send <- messageJSON
			s.sendMessageAck(message.ID, memberID, message.From, "delivered")
		}
	}
	for instanceID, members := range membersByInstance {
		forwardCmd, _ := json.Marshal(map[string]interface{}{
			"type":     "forward_group_message",
			"group_id": groupID,
			"members":  members,
			"payload":  string(messageJSON),
		})
		s.redisClient.Publish(s.ctx, fmt.Sprintf("instance:%s", instanceID), forwardCmd)
	}
	for _, memberID := range offlineMembers {
		memberMsg := message
		memberMsg.To = memberID
		s.storePendingMessage(memberMsg)
	}
	s.addToBatch("messages", message)
	collection.UpdateOne(s.ctx, bson.M{"_id": groupID}, bson.M{"$set": bson.M{"last_message": message.Timestamp}})
}

func (s *Server) handleCreateGroup(client *Client, name string, members []string) {
	group := GroupMetadata{
		ID:        NewUUID(),
		Name:      name,
		Members:   members,
		CreatedAt: time.Now(),
		CreatedBy: client.UserID,
	}
	collection := s.mongoClient.Database("chat").Collection("groups")
	_, err := collection.InsertOne(s.ctx, group)
	if err != nil {
		log.Printf("Error creating group: %v", err)
		return
	}
	userGroupsCollection := s.mongoClient.Database("chat").Collection("user_groups")
	for _, memberID := range members {
		var userGroups struct {
			UserID string   `bson:"user_id"`
			Groups []string `bson:"groups"`
		}
		err := userGroupsCollection.FindOne(s.ctx, bson.M{"user_id": memberID}).Decode(&userGroups)
		if err == mongo.ErrNoDocuments {
			_, err = userGroupsCollection.InsertOne(s.ctx, bson.M{"user_id": memberID, "groups": []string{group.ID}})
		} else if err == nil {
			if !contains(userGroups.Groups, group.ID) {
				userGroups.Groups = append(userGroups.Groups, group.ID)
				userGroupsCollection.UpdateOne(s.ctx, bson.M{"user_id": memberID}, bson.M{"$set": bson.M{"groups": userGroups.Groups}})
			}
		}
		s.connectionsMux.RLock()
		member, ok := s.connections[memberID]
		s.connectionsMux.RUnlock()
		if ok && member.IsConnected && !contains(member.Groups, group.ID) {
			s.connectionsMux.Lock()
			member.Groups = append(member.Groups, group.ID)
			s.connectionsMux.Unlock()
		}
	}
	if !contains(client.Groups, group.ID) {
		s.connectionsMux.Lock()
		client.Groups = append(client.Groups, group.ID)
		s.connectionsMux.Unlock()
	}
	successNotification := CreateNotification("group_created", map[string]interface{}{
		"group_id": group.ID,
		"name":     group.Name,
		"members":  group.Members,
	})
	successJSON, _ := json.Marshal(successNotification)
	client.Send <- successJSON
	event := ChatEvent{
		ID:        NewUUID(),
		Type:      "group_created",
		UserID:    client.UserID,
		Data:      json.RawMessage(mustMarshal(group)),
		Timestamp: time.Now(),
	}
	s.kafkaClient.PublishEvent(event)
}

func (s *Server) handleMessageAck(client *Client, messageID, status string) {
	s.sendMessageAck(messageID, client.UserID, "", status)
	client.removeUnacknowledgedMessage(messageID)
}

func (s *Server) addToBatch(collection string, document interface{}) {
	s.messageBatchMutex.Lock()
	defer s.messageBatchMutex.Unlock()
	s.messageBatch = append(s.messageBatch, map[string]interface{}{
		"collection": collection,
		"document":   document,
	})
	if len(s.messageBatch) >= 100 {
		go s.processBatch()
	}
}

func (s *Server) processBatch() {
	s.messageBatchMutex.Lock()
	if len(s.messageBatch) == 0 {
		s.messageBatchMutex.Unlock()
		return
	}
	batch := s.messageBatch
	s.messageBatch = make([]interface{}, 0, 100)
	s.messageBatchMutex.Unlock()

	collectionBatches := make(map[string][]interface{})
	for _, item := range batch {
		itemMap := item.(map[string]interface{})
		collection := itemMap["collection"].(string)
		document := itemMap["document"]
		collectionBatches[collection] = append(collectionBatches[collection], document)
	}

	for collection, documents := range collectionBatches {
		if len(documents) == 0 {
			continue
		}
		switch collection {
		case "messages":
			messagesCollection := s.mongoClient.Database("chat").Collection("messages")
			_, err := messagesCollection.InsertMany(s.ctx, documents)
			if err != nil {
				log.Printf("Error batch inserting messages: %v", err)
			}
		case "acks":
			messagesCollection := s.mongoClient.Database("chat").Collection("messages")
			for _, doc := range documents {
				ack := doc.(map[string]interface{})
				messageID := ack["message_id"].(string)
				status := ack["status"].(string)
				update := bson.M{"$set": bson.M{"status": status}}
				if status == "delivered" {
					update["$set"].(bson.M)["delivered_at"] = ack["timestamp"]
				} else if status == "read" {
					update["$set"].(bson.M)["read_at"] = ack["timestamp"]
				}
				_, err := messagesCollection.UpdateOne(s.ctx, bson.M{"id": messageID}, update)
				if err != nil {
					log.Printf("Error updating message status: %v", err)
				}
			}
		}
	}
}

func (s *Server) StartBatchProcessor() {
	for {
		select {
		case <-s.batchTicker.C:
			s.processBatch()
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Server) Shutdown() {
	s.cancelFunc()
	s.processBatch()
	s.connectionsMux.Lock()
	for _, client := range s.connections {
		client.Connection.Close()
	}
	s.connectionsMux.Unlock()
	if s.redisPubSub != nil {
		s.redisPubSub.Close()
	}
	s.redisClient.Del(s.ctx, fmt.Sprintf("instances:%s", s.instanceID))
	if s.kafkaClient != nil {
		s.kafkaClient.Close()
	}
	s.redisClient.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s.mongoClient.Disconnect(ctx)
	log.Println("Server resources released")
}

func (s *Server) getNextSequenceNumber(chatID string) int64 {
	s.sequenceNumMutex.Lock()
	defer s.sequenceNumMutex.Unlock()
	seqNum, exists := s.sequenceNumbers[chatID]
	if !exists {
		result, err := s.redisClient.Get(s.ctx, fmt.Sprintf("chat:%s:seq", chatID)).Int64()
		if err == nil {
			seqNum = result
		} else {
			seqNum = 0
		}
	}
	seqNum++
	s.sequenceNumbers[chatID] = seqNum
	s.redisClient.Set(s.ctx, fmt.Sprintf("chat:%s:seq", chatID), seqNum, 0)
	return seqNum
}

func (s *Server) updateUserSequenceNumber(userID string, seqNum int64) {
	s.redisClient.Set(s.ctx, fmt.Sprintf("user:%s:last_seq", userID), seqNum, 24*time.Hour)
	s.connectionsMux.RLock()
	defer s.connectionsMux.RUnlock()
	if client, ok := s.connections[userID]; ok && client.lastSequenceNumber < seqNum {
		client.lastSequenceNumber = seqNum
		client.connInfo.LastSequence = seqNum
	}
}

func (s *Server) updateUserPresence(client *Client) {
	client.LastSeen = time.Now()
	client.updateConnectionInfo()
	s.updateUserPresenceState(client.UserID, true)
}

func (s *Server) handleTypingIndicator(client *Client, toID string) {
	notification := CreateNotification("typing", map[string]interface{}{
		"user_id": client.UserID,
		"to":      toID,
	})
	notificationJSON, _ := json.Marshal(notification)
	isGroup := contains(client.Groups, toID)
	if isGroup {
		s.routeGroupTypingIndicator(client.UserID, toID, notificationJSON)
	} else {
		s.routeDirectTypingIndicator(client.UserID, toID, notificationJSON)
	}
}

func (s *Server) routeDirectTypingIndicator(fromUserID, toUserID string, notification []byte) {
	s.connectionsMux.RLock()
	recipient, ok := s.connections[toUserID]
	s.connectionsMux.RUnlock()
	if ok && recipient.IsConnected {
		recipient.Send <- notification
	} else {
		instanceID, err := s.redisClient.Get(s.ctx, fmt.Sprintf("user:%s:instance", toUserID)).Result()
		if err == nil && instanceID != "" && instanceID != s.instanceID {
			forwardCmd, _ := json.Marshal(map[string]interface{}{
				"type":    "forward_typing",
				"from":    fromUserID,
				"to":      toUserID,
				"payload": string(notification),
			})
			s.redisClient.Publish(s.ctx, fmt.Sprintf("instance:%s", instanceID), forwardCmd)
		}
	}
}

func (s *Server) routeGroupTypingIndicator(fromUserID, groupID string, notification []byte) {
	collection := s.mongoClient.Database("chat").Collection("groups")
	var group GroupMetadata
	err := collection.FindOne(s.ctx, bson.M{"_id": groupID}).Decode(&group)
	if err != nil {
		log.Printf("Error fetching group %s: %v", groupID, err)
		return
	}
	for _, memberID := range group.Members {
		if memberID == fromUserID {
			continue
		}
		s.connectionsMux.RLock()
		member, ok := s.connections[memberID]
		s.connectionsMux.RUnlock()
		if ok && member.IsConnected {
			member.Send <- notification
		} else {
			instanceID, err := s.redisClient.Get(s.ctx, fmt.Sprintf("user:%s:instance", memberID)).Result()
			if err == nil && instanceID != "" && instanceID != s.instanceID {
				forwardCmd, _ := json.Marshal(map[string]interface{}{
					"type":     "forward_group_typing",
					"from":     fromUserID,
					"group_id": groupID,
					"members":  []string{memberID},
					"payload":  string(notification),
				})
				s.redisClient.Publish(s.ctx, fmt.Sprintf("instance:%s", instanceID), forwardCmd)
			}
		}
	}
}

func (s *Server) updateUserPresenceState(userID string, isOnline bool) {
	status := "online"
	if !isOnline {
		status = "offline"
	}
	presenceKey := fmt.Sprintf("user:%s:presence", userID)
	lastSeenKey := fmt.Sprintf("user:%s:last_seen", userID)
	now := time.Now()
	pipe := s.redisClient.Pipeline()
	pipe.Set(s.ctx, presenceKey, status, 24*time.Hour)
	pipe.Set(s.ctx, lastSeenKey, now.Unix(), 24*time.Hour)
	if isOnline {
		pipe.Set(s.ctx, fmt.Sprintf("user:%s:instance", userID), s.instanceID, 24*time.Hour)
	} else {
		pipe.Del(s.ctx, fmt.Sprintf("user:%s:instance", userID))
	}
	pipe.Exec(s.ctx)
}

func (s *Server) handleOnlineStatusCheck(client *Client, targetUserID string) {
	isOnline, lastSeen, _ := s.checkUserOnlineStatus(targetUserID)
	statusResponse := CreateNotification("online_status", map[string]interface{}{
		"user_id":   targetUserID,
		"is_online": isOnline,
		"last_seen": lastSeen.Unix(),
		"timestamp": time.Now().Unix(),
	})
	responseJSON, _ := json.Marshal(statusResponse)
	client.Send <- responseJSON
}

func (s *Server) checkUserOnlineStatus(targetUserID string) (bool, time.Time, error) {
	s.connectionsMux.RLock()
	client, ok := s.connections[targetUserID]
	s.connectionsMux.RUnlock()
	if ok && client.IsConnected {
		return true, client.LastSeen, nil
	}
	instanceID, err := s.redisClient.Get(s.ctx, fmt.Sprintf("user:%s:instance", targetUserID)).Result()
	if err == nil && instanceID != "" {
		instanceActive, _ := s.redisClient.Exists(s.ctx, fmt.Sprintf("instances:%s", instanceID)).Result()
		if instanceActive > 0 {
			lastSeenUnix, err := s.redisClient.Get(s.ctx, fmt.Sprintf("user:%s:last_seen", targetUserID)).Int64()
			if err == nil {
				lastSeen := time.Unix(lastSeenUnix, 0)
				if time.Since(lastSeen) < time.Minute {
					return true, lastSeen, nil
				}
			}
		}
	}
	lastSeenUnix, err := s.redisClient.Get(s.ctx, fmt.Sprintf("user:%s:last_seen", targetUserID)).Int64()
	if err != nil {
		return false, time.Time{}, nil
	}
	return false, time.Unix(lastSeenUnix, 0), nil
}

func (s *Server) validateConnectionStatuses() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			keys, _, _ := s.redisClient.Scan(s.ctx, 0, "user:*:instance", 100).Result()
			for _, key := range keys {
				userID := key[5 : len(key)-9]
				instanceID, _ := s.redisClient.Get(s.ctx, key).Result()
				instanceActive, _ := s.redisClient.Exists(s.ctx, fmt.Sprintf("instances:%s", instanceID)).Result()
				if instanceActive == 0 {
					s.redisClient.Del(s.ctx, key)
					s.redisClient.Set(s.ctx, fmt.Sprintf("user:%s:presence", userID), "offline", 24*time.Hour)
				}
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func getDirectChatID(user1, user2 string) string {
	if user1 < user2 {
		return fmt.Sprintf("direct:%s:%s", user1, user2)
	}
	return fmt.Sprintf("direct:%s:%s", user2, user1)
}

func mustMarshal(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

func (s *Server) handleHistoryRequest(client *Client, chatID string, before int64, limit int, isGroup bool) {
	// Construct MongoDB filter based on chat type
	var filter bson.M
	if isGroup {
		filter = bson.M{"to": chatID, "type": "group"}
	} else {
		filter = bson.M{"$or": []bson.M{
			{"from": client.UserID, "to": chatID, "type": "direct"},
			{"from": chatID, "to": client.UserID, "type": "direct"},
		}}
	}
	if before > 0 {
		filter["timestamp"] = bson.M{"$lt": time.Unix(before, 0)}
	}

	// Query MongoDB
	collection := s.mongoClient.Database("chat").Collection("messages")
	opts := options.Find().SetSort(bson.D{{Key: "timestamp", Value: -1}}).SetLimit(int64(limit))
	cursor, err := collection.Find(s.ctx, filter, opts)
	if err != nil {
		log.Printf("Error fetching history: %v", err)
		return
	}
	defer cursor.Close(s.ctx)

	// Decode results
	var messages []MessagePacket
	for cursor.Next(s.ctx) {
		var message MessagePacket
		if err := cursor.Decode(&message); err != nil {
			log.Printf("Error decoding message: %v", err)
			continue
		}
		messages = append(messages, message)
	}

	// Send response to client
	historyResponse := CreateNotification("history_response", map[string]interface{}{
		"chat_id":  chatID,
		"messages": messages,
		"is_group": isGroup,
	})
	responseJSON, _ := json.Marshal(historyResponse)
	client.Send <- responseJSON
}

func (s *Server) handleVersion(w http.ResponseWriter, r *http.Request) {
	version := map[string]string{
		"version":    "1.0.0", // Change this for each test (e.g., "1.0.1")
		"build_time": time.Now().Format(time.RFC3339),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(version)
}
