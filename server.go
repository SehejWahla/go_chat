package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/golang-jwt/jwt/v4"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func extractDatabaseFromURI(uri string) string {
	lastSlash := strings.LastIndex(uri, "/")
	if lastSlash == -1 {
		return ""
	}
	uri = uri[lastSlash+1:]
	questionMark := strings.Index(uri, "?")
	if questionMark == -1 {
		return uri
	}
	return uri[:questionMark]
}

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
	dbName            string
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

	dbName := extractDatabaseFromURI(mongoURI)
	if dbName == "" {
		return fmt.Errorf("no database name found in MONGO_URI")
	}
	s.dbName = dbName

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
	messagesCollection := s.mongoClient.Database(s.dbName).Collection("messages")
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
	globalDisconnectChannel := "global:disconnect"
	s.redisPubSub = s.redisClient.Subscribe(s.ctx, instanceChannel, "global:notifications", globalDisconnectChannel)
	go s.handleRedisMessages()
	log.Printf("Subscribed to Redis channels: %s, global:notifications, %s", instanceChannel, globalDisconnectChannel)
	return nil
}

// forceDisconnectClient actively terminates a specific connection ID for a user on the current instance.
// It ensures only the targeted connection is affected.
func (s *Server) forceDisconnectClient(userID, connectionIDToDisconnect string) {
	// Lock the mutex to ensure thread-safe access to the connections map
	s.connectionsMux.Lock()

	// Retrieve the client from the connections map and verify it matches the target connection
	clientToDisconnect, ok := s.connections[userID]
	if ok && clientToDisconnect.ConnectionID == connectionIDToDisconnect {
		// Log the start of the force disconnect process
		log.Printf("Force disconnecting client: User %s, Conn %s", userID, connectionIDToDisconnect)

		// Create and send a custom WebSocket close message with code 4001
		closeMessage := websocket.FormatCloseMessage(4001, "Forced disconnect due to new connection")
		err := clientToDisconnect.Connection.WriteControl(
			websocket.CloseMessage,
			closeMessage,
			time.Now().Add(10*time.Second),
		)
		if err != nil {
			// Log any error that occurs while sending the close message
			log.Printf("Error sending close message to %s (%s): %v", userID, connectionIDToDisconnect, err)
		}

		// Remove the client from the connections map and unlock the mutex
		delete(s.connections, userID)
		s.connectionsMux.Unlock()

		// Safely close the send channel with panic recovery
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("Recovered from panic closing send channel for %s (%s): %v", userID, connectionIDToDisconnect, r)
				}
			}()
			select {
			case _, chanOk := <-clientToDisconnect.Send:
				if !chanOk {
					log.Printf("Send channel for %s (%s) was already closed.", userID, connectionIDToDisconnect)
					return
				}
			default:
				close(clientToDisconnect.Send)
				log.Printf("Closed send channel for %s (%s)", userID, connectionIDToDisconnect)
			}
		}()

		// Close the WebSocket connection
		err = clientToDisconnect.Connection.Close()
		if err != nil {
			// Log any error that occurs while closing the connection
			log.Printf("Error closing websocket connection for %s (%s): %v", userID, connectionIDToDisconnect, err)
		}

		// Clean up the connection-specific Redis key
		connInfoKey := fmt.Sprintf("conn:%s", connectionIDToDisconnect)
		deletedCount, delErr := s.redisClient.Del(s.ctx, connInfoKey).Result()
		if delErr != nil {
			// Log a warning if Redis key deletion fails
			log.Printf("WARN: Failed to delete Redis key %s during force disconnect: %v", connInfoKey, delErr)
		} else if deletedCount > 0 {
			// Log successful deletion of the Redis key
			log.Printf("Deleted Redis key %s", connInfoKey)
		}

		// Log the completion of the force disconnect process
		log.Printf("Forced disconnect completed for User %s, Conn %s", userID, connectionIDToDisconnect)
	} else {
		// Unlock the mutex and log if the client isn't found or doesn't match
		s.connectionsMux.Unlock()
		log.Printf("Disconnect request for User %s, Conn %s ignored: User not found locally or different connection.", userID, connectionIDToDisconnect)
	}
}

func (s *Server) handleRedisMessages() {
	ch := s.redisPubSub.Channel() // Use Channel() for better control

	for {
		select {
		case <-s.ctx.Done():
			log.Println("Redis PubSub handler stopping.")
			err := s.redisPubSub.Close() // Ensure pubsub is closed on shutdown
			if err != nil {
				log.Printf("Error closing Redis PubSub: %v", err)
			}
			return
		case msg := <-ch:
			if msg == nil {
				// This case typically occurs if the PubSub connection is closed externally or encounters a severe error.
				log.Println("Redis PubSub channel returned nil message. Attempting to re-subscribe...")
				// Optional: Add logic to attempt re-subscription with backoff
				time.Sleep(5 * time.Second) // Prevent tight loop on persistent error
				// Re-initialize subscription
				err := s.initRedisSubscriptions()
				if err != nil {
					log.Printf("FATAL: Failed to re-subscribe to Redis after channel closure: %v", err)
					// Consider shutting down the server instance if Redis is critical and cannot reconnect
					s.Shutdown() // Or trigger a more graceful shutdown sequence
				} else {
					log.Println("Successfully re-subscribed to Redis.")
					// Important: The old goroutine exits here, a new one is started by initRedisSubscriptions
					return
				}
			}

			// Handle instance-specific messages (forwarding, disconnects)
			if msg.Channel == fmt.Sprintf("instance:%s", s.instanceID) {
				var message map[string]interface{}
				if err := json.Unmarshal([]byte(msg.Payload), &message); err != nil {
					log.Printf("Error unmarshaling Redis instance message payload '%s': %v", msg.Payload, err)
					continue
				}

				messageType, typeOk := message["type"].(string)
				if !typeOk {
					log.Printf("Received Redis message on instance channel without 'type' field: %+v", message)
					continue
				}

				switch messageType {
				case "forward_message":
					recipientID, okTo := message["to"].(string)
					payload, okPayload := message["payload"].(string)
					if !okTo || !okPayload {
						log.Printf("Invalid 'forward_message' format: %+v", message)
						continue
					}

					var msgPacket MessagePacket
					if err := json.Unmarshal([]byte(payload), &msgPacket); err != nil {
						log.Printf("Error unmarshaling forwarded message payload: %v", err)
						continue
					}
					s.connectionsMux.RLock()
					recipient, ok := s.connections[recipientID]
					s.connectionsMux.RUnlock()
					if ok && recipient.IsConnected {
						// Use non-blocking send to prevent deadlock if client channel is full
						select {
						case recipient.Send <- []byte(payload):
							s.sendMessageAck(msgPacket.ID, recipientID, msgPacket.From, "delivered")
						default:
							log.Printf("WARN: Send channel full for user %s (Conn %s), dropping forwarded message %s", recipientID, recipient.ConnectionID, msgPacket.ID)
							// Consider alternative handling for full buffers (e.g., requeueing, notifying sender)
						}
					} else {
						log.Printf("User %s not found locally for forwarded message %s, might be offline or disconnected", recipientID, msgPacket.ID)
					}

				case "forward_group_message":
					groupID, okGroup := message["group_id"].(string)
					payload, okPayload := message["payload"].(string)
					membersPayload, okMembers := message["members"]
					if !okGroup || !okPayload || !okMembers {
						log.Printf("Invalid 'forward_group_message' format: %+v", message)
						continue
					}

					var members []string
					if membersInterface, ok := membersPayload.([]interface{}); ok {
						for _, m := range membersInterface {
							if memberStr, okStr := m.(string); okStr {
								members = append(members, memberStr)
							} else {
								log.Printf("WARN: Non-string member found in forward_group_message members list: %v", m)
							}
						}
					} else {
						log.Printf("Error: invalid members type (%T) in forward_group_message for group %s", membersPayload, groupID)
						continue
					}

					if len(members) == 0 {
						log.Printf("WARN: Received forward_group_message for group %s with empty resolved members list.", groupID)
						continue // No one on this instance to send to
					}

					var msgPacket MessagePacket
					if err := json.Unmarshal([]byte(payload), &msgPacket); err != nil {
						log.Printf("Error unmarshaling forwarded group message payload for group %s: %v", groupID, err)
						continue
					}

					for _, memberID := range members {
						s.connectionsMux.RLock()
						recipient, ok := s.connections[memberID]
						s.connectionsMux.RUnlock() // Unlock inside loop after check
						if ok && recipient.IsConnected {
							// Use non-blocking send
							select {
							case recipient.Send <- []byte(payload):
								s.sendMessageAck(msgPacket.ID, memberID, msgPacket.From, "delivered")
							default:
								log.Printf("WARN: Send channel full for user %s (Conn %s), dropping forwarded group message %s", memberID, recipient.ConnectionID, msgPacket.ID)
							}
						} else {
							// This is expected if the member disconnected just after the routing decision was made
							log.Printf("User %s not found locally for forwarded group message %s", memberID, msgPacket.ID)
						}
					}

				case "forward_notification":
					toUserID, okTo := message["to"].(string)
					payload, okPayload := message["payload"].(string)
					if !okTo || !okPayload {
						log.Printf("Invalid 'forward_notification' format: %+v", message)
						continue
					}

					s.connectionsMux.RLock()
					client, ok := s.connections[toUserID]
					s.connectionsMux.RUnlock()
					if ok && client.IsConnected {
						select {
						case client.Send <- []byte(payload):
							// Notification sent
						default:
							log.Printf("WARN: Send channel full for user %s (Conn %s), dropping forwarded notification", toUserID, client.ConnectionID)
						}
					} else {
						log.Printf("User %s not found locally for forwarded notification", toUserID)
					}

				case "disconnect_user":
					userID, okUser := message["user_id"].(string)
					connIDToDisconnect, okConn := message["connection_id"].(string)
					if okUser && okConn {
						log.Printf("Received request to disconnect User %s (Conn %s) on this instance", userID, connIDToDisconnect)
						s.forceDisconnectClient(userID, connIDToDisconnect)
					} else {
						log.Printf("Error parsing disconnect_user message: %+v", message)
					}

				case "forward_typing":
					toUserID, okTo := message["to"].(string)
					payload, okPayload := message["payload"].(string)
					// fromUserID might also be useful for context, though not strictly needed to forward
					// fromUserID, _ := message["from"].(string)
					if !okTo || !okPayload {
						log.Printf("Invalid 'forward_typing' format: %+v", message)
						continue
					}
					s.connectionsMux.RLock()
					recipient, ok := s.connections[toUserID]
					s.connectionsMux.RUnlock()
					if ok && recipient.IsConnected {
						select {
						case recipient.Send <- []byte(payload):
							// Typing indicator sent
						default:
							log.Printf("WARN: Send channel full for user %s (Conn %s), dropping forwarded typing indicator", toUserID, recipient.ConnectionID)
						}
					}

				case "forward_group_typing":
					groupID, okGroup := message["group_id"].(string)
					payload, okPayload := message["payload"].(string)
					membersPayload, okMembers := message["members"]
					// fromUserID, _ := message["from"].(string) // Optional context
					if !okGroup || !okPayload || !okMembers {
						log.Printf("Invalid 'forward_group_typing' format: %+v", message)
						continue
					}

					var members []string
					if membersInterface, ok := membersPayload.([]interface{}); ok {
						for _, m := range membersInterface {
							if memberStr, okStr := m.(string); okStr {
								members = append(members, memberStr)
							}
						}
					} else {
						log.Printf("Error: invalid members type (%T) in forward_group_typing for group %s", membersPayload, groupID)
						continue
					}

					if len(members) == 0 {
						continue
					}

					for _, memberID := range members {
						s.connectionsMux.RLock()
						recipient, ok := s.connections[memberID]
						s.connectionsMux.RUnlock()
						if ok && recipient.IsConnected {
							select {
							case recipient.Send <- []byte(payload):
								// Group typing indicator sent
							default:
								log.Printf("WARN: Send channel full for user %s (Conn %s), dropping forwarded group typing indicator", memberID, recipient.ConnectionID)
							}
						}
					}

				default:
					log.Printf("Received unknown message type '%s' on instance channel: %+v", messageType, message)

				} // end switch messageType

			} else if msg.Channel == "global:notifications" {
				// Handle global notifications if necessary
				log.Printf("Received global notification: %s", msg.Payload)
				// Example: Propagate to all clients on this instance if needed
				// s.connectionsMux.RLock()
				// for _, client := range s.connections {
				//  select {
				//  case client.Send <- []byte(msg.Payload):
				//  default: log.Printf("WARN: Send channel full for user %s during global notification broadcast", client.UserID)
				//  }
				// }
				// s.connectionsMux.RUnlock()

			} else {
				log.Printf("Received message on unexpected Redis channel: %s", msg.Channel)
			}
		} // end select
	} // end for
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
	tokenStr := r.URL.Query().Get("token")

	if userID == "" || tokenStr == "" {
		http.Error(w, "Missing user_id or token", http.StatusUnauthorized)
		return
	}

	// Parse and verify the JWT
	claims := &jwt.MapClaims{}
	token, err := jwt.ParseWithClaims(tokenStr, claims, func(token *jwt.Token) (interface{}, error) {
		return []byte(os.Getenv("ACCESS_TOKEN_SECRET")), nil
	})
	if err != nil || !token.Valid {
		http.Error(w, "Invalid or expired token", http.StatusUnauthorized)
		return
	}

	// Extract email from token and ensure it matches the provided user_id
	email, ok := (*claims)["email"].(string)
	if !ok || email != userID {
		http.Error(w, "Token email does not match user_id", http.StatusUnauthorized)
		return
	}

	// Upgrade the connection
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Error upgrading connection: %v", err)
		return
	}

	// Create client with the token
	client := NewClient(userID, conn, s, tokenStr)
	s.registerClient(client)
	s.deliverPendingMessages(client)
	s.loadUserGroups(client)

	go client.readPump()
	go client.writePump()
}

func (s *Server) deliverPendingMessages(client *Client) {
	collection := s.mongoClient.Database(s.dbName).Collection("pending_messages")
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
	userID := client.UserID
	newConnectionID := client.ConnectionID
	newInstanceID := s.instanceID
	userConnKey := fmt.Sprintf("user:%s:conn", userID)
	userInstanceKey := fmt.Sprintf("user:%s:instance", userID)
	connInfoKey := fmt.Sprintf("conn:%s", newConnectionID)
	presenceKey := fmt.Sprintf("user:%s:presence", userID)
	lastSeenKey := fmt.Sprintf("user:%s:last_seen", userID)

	log.Printf("Attempting to register client: User %s, New Conn %s, Instance %s", userID, newConnectionID, newInstanceID)

	// --- Atomically update Redis and get previous connection info ---
	pipe := s.redisClient.Pipeline()
	prevConnIDCmd := pipe.GetSet(s.ctx, userConnKey, newConnectionID)
	prevInstanceIDCmd := pipe.GetSet(s.ctx, userInstanceKey, newInstanceID)
	// Set initial connection info with expiration
	client.connInfo.InstanceID = newInstanceID // Ensure connInfo has the correct instance ID before saving
	connectionData, _ := json.Marshal(client.connInfo)
	pipe.Set(s.ctx, connInfoKey, connectionData, 24*time.Hour)
	// Set initial presence
	pipe.Set(s.ctx, presenceKey, "online", 24*time.Hour)
	pipe.Set(s.ctx, lastSeenKey, time.Now().Unix(), 24*time.Hour)

	// Execute the pipeline
	_, execErr := pipe.Exec(s.ctx) // Rename err to execErr to avoid confusion later

	// --- MODIFIED ERROR HANDLING for pipe.Exec ---
	if execErr != nil {
		if errors.Is(execErr, redis.Nil) {
			// Treat redis.Nil from Exec as a WARNING, not a fatal error for the pipeline itself.
			// This might indicate non-existent keys were GetSet, which is expected for new users.
			// We will proceed to check individual command results.
			log.Printf("WARN: Redis pipeline Exec returned redis.Nil for User %s, Conn %s. Proceeding to check individual command results. Error: %v", userID, newConnectionID, execErr)
		} else {
			// For any other error from Exec (network, transaction error), treat it as fatal.
			log.Printf("ERROR: Redis pipeline Exec failed during registration for User %s, Conn %s: %v", userID, newConnectionID, execErr)
			client.Connection.Close() // Close the new connection
			// Attempt to clean up the potentially half-set keys (best effort)
			_, delErr := s.redisClient.Del(s.ctx, userConnKey, userInstanceKey, connInfoKey, presenceKey, lastSeenKey).Result()
			if delErr != nil {
				log.Printf("WARN: Failed to cleanup keys after pipeline Exec error for user %s: %v", userID, delErr)
			}
			return // Stop registration
		}
	}
	// --- END MODIFIED ERROR HANDLING ---

	// Now check the results from the individual commands within the pipeline
	prevConnID, errConn := prevConnIDCmd.Result()
	isNilConnErr := errors.Is(errConn, redis.Nil) // Check if the key didn't exist before GetSet
	if errConn != nil && !isNilConnErr {
		// Log error if GetSet failed for a reason other than Nil
		log.Printf("ERROR: Failed to get previous connection ID result from Redis pipeline for User %s: %v. State might be inconsistent.", userID, errConn)
		// Consider if this warrants closing the connection - depends on how critical GetSet success is.
		// For now, log and continue, as other parts of the pipeline likely succeeded.
	}

	prevInstanceID, errInstance := prevInstanceIDCmd.Result()
	isNilInstanceErr := errors.Is(errInstance, redis.Nil)
	if errInstance != nil && !isNilInstanceErr {
		// Log error if GetSet failed for a reason other than Nil
		log.Printf("ERROR: Failed to get previous instance ID result from Redis pipeline for User %s: %v. State might be inconsistent.", userID, errInstance)
	}

	// Log results (even if there were non-fatal errors getting them)
	log.Printf("Redis GETSET results: Prev Conn '%s' (Nil err: %v), Prev Instance '%s' (Nil err: %v)",
		prevConnID, isNilConnErr, prevInstanceID, isNilInstanceErr)

	// --- Handle the previous connection ---
	// Only proceed if there *was* a previous connection ID returned (errConn is nil or redis.Nil),
	// and it's different from the new one.
	if (errConn == nil || isNilConnErr) && prevConnID != "" && prevConnID != newConnectionID {
		log.Printf("Found previous connection: User %s, Conn %s, Instance %s", userID, prevConnID, prevInstanceID)

		// Determine where the previous connection was
		// Use the retrieved prevInstanceID. Check errInstance as well for robustness.
		if prevInstanceID == newInstanceID && (errInstance == nil || isNilInstanceErr) {
			// Previous connection was on THIS instance
			log.Printf("Previous connection for %s (%s) was on this instance. Forcing disconnect.", userID, prevConnID)
			s.forceDisconnectClient(userID, prevConnID)
		} else if prevInstanceID != "" && (errInstance == nil || isNilInstanceErr) {
			// Previous connection was on a DIFFERENT, known instance
			log.Printf("Previous connection for %s (%s) was on different instance (%s). Sending disconnect signal.", userID, prevConnID, prevInstanceID)
			disconnectMsg := map[string]interface{}{
				"type":          "disconnect_user",
				"user_id":       userID,
				"connection_id": prevConnID,
			}
			payload, _ := json.Marshal(disconnectMsg)
			targetInstanceChannel := fmt.Sprintf("instance:%s", prevInstanceID)
			errPub := s.redisClient.Publish(s.ctx, targetInstanceChannel, payload).Err()
			if errPub != nil {
				log.Printf("ERROR: Failed to publish disconnect signal to instance %s channel %s: %v", prevInstanceID, targetInstanceChannel, errPub)
			}
		} else {
			// Previous connection ID existed, but instance ID was missing, redis.Nil, or had another error.
			log.Printf("WARNING: Found previous connection ID %s for user %s, but previous instance ID was '%s' (Instance Err: %v). Attempting local disconnect and Redis cleanup.", prevConnID, userID, prevInstanceID, errInstance)
			// Attempt local disconnect as a fallback.
			s.forceDisconnectClient(userID, prevConnID)
			// Also, explicitly delete the potentially orphaned conn info key in Redis for the old connection.
			_, delErr := s.redisClient.Del(s.ctx, fmt.Sprintf("conn:%s", prevConnID)).Result()
			if delErr != nil {
				log.Printf("WARN: Failed to delete orphaned conn key %s for user %s: %v", prevConnID, userID, delErr)
			}
		}
	} else if (errConn == nil || isNilConnErr) && prevConnID == newConnectionID {
		// This case means GetSet returned the *same* ID we just set. Should be rare with UUIDs.
		log.Printf("INFO: Previous connection ID %s is the same as the new one for user %s. No disconnect action needed.", prevConnID, userID)
	}

	// --- Register the NEW client locally in the map ---
	s.connectionsMux.Lock()
	existingClient, exists := s.connections[userID]
	if exists && existingClient.ConnectionID != newConnectionID {
		log.Printf("WARNING: Stale client %s found in local map for user %s during registration of %s. Closing stale local client.", existingClient.ConnectionID, userID, newConnectionID)
		staleClient := existingClient
		delete(s.connections, userID)
		s.connectionsMux.Unlock()

		// Close resources outside lock
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("Recovered from panic closing stale client send channel for %s (%s): %v", userID, staleClient.ConnectionID, r)
				}
			}()
			close(staleClient.Send)
		}()
		staleClient.Connection.Close()

		s.connectionsMux.Lock() // Re-acquire lock
		s.connections[userID] = client
		s.connectionsMux.Unlock()

	} else {
		// No existing client, or existing client IS the new one (harmless overwrite).
		if exists {
			log.Printf("INFO: Client %s for user %s already in map during registration. Overwriting.", newConnectionID, userID)
		}
		s.connections[userID] = client
		s.connectionsMux.Unlock()
	}

	log.Printf("Client registered successfully: User %s, Conn %s, Instance %s", userID, newConnectionID, newInstanceID)
	// Note: Presence, connection info, user:conn, user:instance keys were set/updated in the pipeline.
}

func (s *Server) unregisterClient(client *Client) {
	userID := client.UserID
	connID := client.ConnectionID // The ID of the connection triggering this unregister

	log.Printf("Unregistering client: User %s, Conn %s", userID, connID)

	// --- Local Map Cleanup ---
	s.connectionsMux.Lock()
	// Verify that the client being unregistered is *still* the one in the map for this user.
	// This prevents accidentally removing a *newer* client if a disconnect/reconnect race occurred,
	// and the readPump for the *old* connection exits slightly later.
	registeredClient, ok := s.connections[userID]
	if ok && registeredClient.ConnectionID == connID {
		// It's the correct client, remove it from the map.
		log.Printf("Removing client %s (%s) from local map.", userID, connID)
		delete(s.connections, userID)
		s.connectionsMux.Unlock() // Unlock after modifying map

		// Close the send channel *after* removing from map to prevent new writes. Check if already closed.
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("Recovered from panic closing send channel during unregister for %s (%s): %v", userID, connID, r)
				}
			}()
			// Assume channel is non-nil from constructor, attempt close. Recover handles double close.
			// More robust check omitted for brevity, rely on recover.
			close(client.Send)
			log.Printf("Closed send channel during unregister for %s (%s)", userID, connID)
		}()

	} else {
		// The client in the map is different or doesn't exist. Don't remove it.
		s.connectionsMux.Unlock() // Unlock as we didn't modify the map
		if ok {
			log.Printf("Skipping removal of %s (%s) from map during unregister: Found different connection %s.", userID, connID, registeredClient.ConnectionID)
		} else {
			log.Printf("Skipping removal of %s (%s) from map during unregister: User not found.", userID, connID)
		}
		// If the client isn't the one in the map, we MUST NOT proceed with Redis cleanup below,
		// as doing so could interfere with the state of the currently active connection.
		// Also, don't close the Send channel of the passed 'client' object if it wasn't the registered one.
		log.Printf("Unregister finished early for %s (%s) as it was not the active client in the map.", userID, connID)
		return // Exit the function early
	}

	// --- Update Local Client State (Optional) ---
	// This client object is now effectively dead.
	client.IsConnected = false

	// --- Conditional Redis Cleanup ---
	// Only clear the user-level Redis keys (instance, presence) if THIS connection
	// (`connID`) is still the "active" one according to the `user:<userID>:conn` key in Redis.
	// This prevents an old, slow unregister call from wiping the state set by a newer connection.
	userConnKey := fmt.Sprintf("user:%s:conn", userID)
	currentConnIDInRedis, err := s.redisClient.Get(s.ctx, userConnKey).Result()

	if err == nil && currentConnIDInRedis == connID {
		// This connection IS still the active one in Redis. Perform cleanup.
		log.Printf("Client %s (%s) is still the active one in Redis. Clearing instance/presence.", userID, connID)
		pipe := s.redisClient.Pipeline()
		pipe.Del(s.ctx, fmt.Sprintf("user:%s:instance", userID))
		// Set presence to offline and update last seen one last time
		pipe.Set(s.ctx, fmt.Sprintf("user:%s:presence", userID), "offline", 24*time.Hour)
		pipe.Set(s.ctx, fmt.Sprintf("user:%s:last_seen", userID), time.Now().Unix(), 24*time.Hour)
		// We can also delete the user:conn key now that we know it matched and we're cleaning up
		pipe.Del(s.ctx, userConnKey)

		_, pipeErr := pipe.Exec(s.ctx)
		if pipeErr != nil {
			log.Printf("WARN: Redis pipeline failed during conditional cleanup for %s (%s): %v", userID, connID, pipeErr)
		} else {
			log.Printf("Cleared instance/presence/conn keys in Redis for %s (%s).", userID, connID)
		}

	} else if errors.Is(err, redis.Nil) {
		// The user:conn key doesn't exist in Redis at all. This implies cleanup might have already happened
		// or the user never fully registered. Ensure presence is marked offline just in case.
		log.Printf("No active connection found in Redis for %s during unregister of %s. Ensuring presence is offline.", userID, connID)
		pipe := s.redisClient.Pipeline()
		pipe.Set(s.ctx, fmt.Sprintf("user:%s:presence", userID), "offline", 24*time.Hour)
		pipe.Set(s.ctx, fmt.Sprintf("user:%s:last_seen", userID), time.Now().Unix(), 24*time.Hour)
		// If instance key exists without conn key, remove it
		pipe.Del(s.ctx, fmt.Sprintf("user:%s:instance", userID))
		_, pipeErr := pipe.Exec(s.ctx)
		if pipeErr != nil {
			log.Printf("WARN: Redis pipeline failed during Nil-conn cleanup for %s (%s): %v", userID, connID, pipeErr)
		}

	} else if err != nil {
		// Failed to check Redis, log the error but don't attempt cleanup without knowing the state.
		log.Printf("WARN: Failed to check current Redis connection for %s during unregister of %s: %v. Skipping Redis user-level cleanup.", userID, connID, err)
	} else {
		// The connection ID in Redis (currentConnIDInRedis) does NOT match the one being unregistered (connID).
		// This means a newer connection is already active. DO NOT touch the shared user keys.
		log.Printf("Skipping Redis user-level cleanup for %s (%s): Newer connection %s is active.", userID, connID, currentConnIDInRedis)
	}

	// --- Always Clean Up Connection-Specific Redis Key ---
	// Regardless of whether it was the active connection, the specific `conn:<connID>` key should be removed.
	connInfoKey := fmt.Sprintf("conn:%s", connID)
	deletedCount, delErr := s.redisClient.Del(s.ctx, connInfoKey).Result()
	if delErr != nil {
		log.Printf("WARN: Failed to delete Redis key %s during unregister: %v", connInfoKey, delErr)
	} else if deletedCount > 0 {
		log.Printf("Deleted connection-specific Redis key %s", connInfoKey)
	}

	log.Printf("Client unregister process finished for: User %s, Conn %s", userID, connID)
}

// updateConnectionInfo updates secondary details like last_seen, last_sequence, and the specific connection info key in Redis.
// It should NOT modify the primary user:instance or user:presence keys, which are handled by register/unregister.
func (s *Server) updateConnectionInfo(connInfo UserConnection) {
	// Ensure the connection info object itself is up-to-date before marshaling
	connInfo.LastSeen = time.Now() // Update LastSeen right before saving
	connInfo.Connected = true      // Assume still connected if this is called from activity

	connectionData, err := json.Marshal(connInfo)
	if err != nil {
		log.Printf("ERROR: Failed to marshal connection info for Conn %s: %v", connInfo.ConnectionID, err)
		return
	}

	// Update connection specific details with expiration
	connInfoKey := fmt.Sprintf("conn:%s", connInfo.ConnectionID)
	// Update user-level details (last seen, sequence) - Use Pipelining
	userLastSeenKey := fmt.Sprintf("user:%s:last_seen", connInfo.UserID)
	userLastSeqKey := fmt.Sprintf("user:%s:last_seq", connInfo.UserID)

	pipe := s.redisClient.Pipeline()
	pipe.Set(s.ctx, connInfoKey, connectionData, 24*time.Hour)
	pipe.Set(s.ctx, userLastSeenKey, connInfo.LastSeen.Unix(), 24*time.Hour)
	if connInfo.LastSequence > 0 { // Only update sequence if it's meaningful
		pipe.Set(s.ctx, userLastSeqKey, connInfo.LastSequence, 24*time.Hour)
	}

	// Optionally update presence ONLY if setting to 'online' (but register/unregister are primary)
	// pipe.Set(s.ctx, fmt.Sprintf("user:%s:presence", connInfo.UserID), "online", 24*time.Hour)

	_, pipeErr := pipe.Exec(s.ctx)
	if pipeErr != nil {
		log.Printf("WARN: Error executing pipeline for updateConnectionInfo for user %s, conn %s: %v", connInfo.UserID, connInfo.ConnectionID, pipeErr)
	}
}

func (s *Server) loadUserGroups(client *Client) {
	collection := s.mongoClient.Database(s.dbName).Collection("user_groups")
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
	collection := s.mongoClient.Database(s.dbName).Collection("pending_messages")
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
		collection := s.mongoClient.Database(s.dbName).Collection("messages")
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
	collection := s.mongoClient.Database(s.dbName).Collection("messages")
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
	collection := s.mongoClient.Database(s.dbName).Collection("groups")
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
	collection := s.mongoClient.Database(s.dbName).Collection("groups")
	_, err := collection.InsertOne(s.ctx, group)
	if err != nil {
		log.Printf("Error creating group: %v", err)
		return
	}
	userGroupsCollection := s.mongoClient.Database(s.dbName).Collection("user_groups")
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
			messagesCollection := s.mongoClient.Database(s.dbName).Collection("messages")
			_, err := messagesCollection.InsertMany(s.ctx, documents)
			if err != nil {
				log.Printf("Error batch inserting messages: %v", err)
			}
		case "acks":
			messagesCollection := s.mongoClient.Database(s.dbName).Collection("messages")
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
	collection := s.mongoClient.Database(s.dbName).Collection("groups")
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

// updateUserPresenceState updates the user's presence status ('online'/'offline') and last_seen time in Redis.
// It's called during registration, unregistration, and potentially periodic heartbeats.
func (s *Server) updateUserPresenceState(userID string, isOnline bool) {
	status := "offline"
	if isOnline {
		status = "online"
	}
	presenceKey := fmt.Sprintf("user:%s:presence", userID)
	lastSeenKey := fmt.Sprintf("user:%s:last_seen", userID)

	// Get current time for last_seen
	now := time.Now().Unix()

	// Use a pipeline to set both keys atomically (relative to Redis execution)
	pipe := s.redisClient.Pipeline()
	pipe.Set(s.ctx, presenceKey, status, 24*time.Hour) // Set presence with expiration
	pipe.Set(s.ctx, lastSeenKey, now, 24*time.Hour)    // Update last_seen with expiration

	// Execute the pipeline
	_, err := pipe.Exec(s.ctx)
	if err != nil {
		log.Printf("WARN: Error executing Redis pipeline for updateUserPresenceState for user %s to %s: %v", userID, status, err)
	} else {
		log.Printf("Updated presence for user %s to %s (Last Seen: %d)", userID, status, now)
	}

	// Optional: Broadcast presence change via Pub/Sub if needed by other services/clients
	/*
		presenceUpdate := map[string]interface{}{
		    "type": "presence_update",
		    "user_id": userID,
		    "status": status,
		    "last_seen": now,
		}
		payload, _ := json.Marshal(presenceUpdate)
		errPub := s.redisClient.Publish(s.ctx, "global:presence", payload).Err()
		if errPub != nil {
		    log.Printf("WARN: Failed to publish presence update for user %s: %v", userID, errPub)
		}
	*/
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
	collection := s.mongoClient.Database(s.dbName).Collection("messages")
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
