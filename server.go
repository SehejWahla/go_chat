// server.go

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

type AckInfo struct {
	Sender string `json:"sender"`
	ChatID string `json:"chatID"`
}

const ackInfoTTL = 5 * time.Minute

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
				log.Println("Redis PubSub channel returned nil message. Attempting to re-subscribe...")
				time.Sleep(5 * time.Second) // Prevent tight loop on persistent error
				err := s.initRedisSubscriptions()
				if err != nil {
					log.Printf("FATAL: Failed to re-subscribe to Redis after channel closure: %v", err)
					s.Shutdown()
				} else {
					log.Println("Successfully re-subscribed to Redis.")
				}
				// The previous goroutine implicitly exits when the channel closes or returns nil.
				// A new one is started by initRedisSubscriptions if successful.
				// If re-subscribe fails and Shutdown is called, the context cancellation will stop this loop eventually.
				// If re-subscribe succeeds, this goroutine *should* exit because the channel `ch` it's reading from
				// is associated with the *closed* PubSub object. The new goroutine reads from the new channel.
				// Adding an explicit return here after re-subscribe attempt might be safer.
				return // Exit this goroutine after attempting re-subscribe.
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
						select {
						case recipient.Send <- []byte(payload):
							// *** FIX: Call new signature with originalChatID (msgPacket.To) ***
							s.sendMessageAck(msgPacket.ID, recipientID, msgPacket.From, msgPacket.To, "delivered")
						default:
							log.Printf("WARN: Send channel full for user %s (Conn %s), dropping forwarded message %s", recipientID, recipient.ConnectionID, msgPacket.ID)
						}
					} else {
						// User might have disconnected between routing decision and message arrival here
						// log.Printf("User %s not found locally for forwarded message %s", recipientID, msgPacket.ID)
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
							} // Silently skip non-strings
						}
					} else {
						log.Printf("Error: invalid members type (%T) in forward_group_message for group %s", membersPayload, groupID)
						continue
					}

					if len(members) == 0 {
						// This is expected if no members of the group are on this instance
						// log.Printf("WARN: Received forward_group_message for group %s with empty resolved members list.", groupID)
						continue
					}

					var msgPacket MessagePacket
					if err := json.Unmarshal([]byte(payload), &msgPacket); err != nil {
						log.Printf("Error unmarshaling forwarded group message payload for group %s: %v", groupID, err)
						continue
					}

					for _, memberID := range members {
						s.connectionsMux.RLock()
						recipient, ok := s.connections[memberID]
						s.connectionsMux.RUnlock()
						if ok && recipient.IsConnected {
							select {
							case recipient.Send <- []byte(payload):
								// *** FIX: Call new signature with originalChatID (groupID) ***
								s.sendMessageAck(msgPacket.ID, memberID, msgPacket.From, groupID, "delivered")
							default:
								log.Printf("WARN: Send channel full for user %s (Conn %s), dropping forwarded group message %s", memberID, recipient.ConnectionID, msgPacket.ID)
							}
						} else {
							// User might have disconnected
							// log.Printf("User %s not found locally for forwarded group message %s", memberID, msgPacket.ID)
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
						default:
							log.Printf("WARN: Send channel full for user %s (Conn %s), dropping forwarded notification", toUserID, client.ConnectionID)
						}
					} else { /* User not local */
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
						default:
							log.Printf("WARN: Send channel full for user %s, dropping typing indicator", toUserID)
						}
					}

				case "forward_group_typing":
					groupID, okGroup := message["group_id"].(string)
					payload, okPayload := message["payload"].(string)
					membersPayload, okMembers := message["members"]
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
						log.Printf("Error: invalid members type (%T) for group %s", membersPayload, groupID)
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
							default:
								log.Printf("WARN: Send channel full for user %s, dropping group typing indicator", memberID)
							}
						}
					}

				default:
					log.Printf("Received unknown message type '%s' on instance channel: %+v", messageType, message)

				} // end switch messageType

			} else if msg.Channel == "global:notifications" {
				log.Printf("Received global notification: %s", msg.Payload)
				// Example: Propagate to all clients on this instance if needed
				// s.broadcastToLocalClients([]byte(msg.Payload))

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
		log.Printf("Error fetching pending: %v", err)
		return
	}
	defer cursor.Close(s.ctx)
	var deliveredMsgIDs []string
	for cursor.Next(s.ctx) {
		var message MessagePacket
		if err := cursor.Decode(&message); err != nil {
			log.Printf("Error decoding pending: %v", err)
			continue
		}
		// Mark as delivered when sending from pending queue
		message.Status = "delivered"
		message.DeliveredAt = time.Now()
		messageJSON, _ := json.Marshal(message)
		delivered := false
		select {
		case client.Send <- messageJSON:
			delivered = true
		default:
			log.Printf("WARN: Send channel full for user %s while delivering pending message %s", client.UserID, message.ID)
			// Optionally break or implement retry/requeue logic for pending messages if buffer is full
		}

		// Only ack and delete if successfully sent to client buffer
		if delivered {
			deliveredMsgIDs = append(deliveredMsgIDs, message.ID)
			// Call NEW signature: messageID, ackingUser, originalSender, originalChatID (recipient), status
			s.sendMessageAck(message.ID, client.UserID, message.From, message.To, "delivered")
		}
	}
	if len(deliveredMsgIDs) > 0 {
		_, err := collection.DeleteMany(s.ctx, bson.M{"id": bson.M{"$in": deliveredMsgIDs}})
		if err != nil {
			log.Printf("Error deleting delivered pending messages: %v", err)
		} else {
			log.Printf("Deleted %d pending messages for user %s", len(deliveredMsgIDs), client.UserID)
		}
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
	message := MessagePacket{ID: NewUUID(), Type: "direct", From: client.UserID, To: toUserID, Content: content, Timestamp: time.Now(), Status: "sent", ClientMsgID: clientMsgID, SequenceNum: seqNum}
	messageJSON, _ := json.Marshal(message)
	// Send back to sender *immediately* with 'sent' status (non-blocking)
	select {
	case client.Send <- messageJSON:
		client.addUnacknowledgedMessage(message) // Track only if successfully sent to own buffer
	default:
		log.Printf("WARN: Send channel full for sender %s, dropping self-sent message %s", client.UserID, message.ID)
		// Consider if an error should be sent back to the client here
	}
	// Route to recipient and add to DB batch
	s.routeDirectMessage(message)
	s.updateUserSequenceNumber(client.UserID, seqNum)
}

func (s *Server) routeDirectMessage(message MessagePacket) {
	recipientID := message.To
	// Send copy with original 'sent' status
	// The recipient's client will send back 'delivered'/'read' acks
	recipientCopy := message

	// *** Store Ack Info in Redis before batching/routing ***
	ackInfo := AckInfo{Sender: message.From, ChatID: message.To}
	ackInfoJSON, errMarshal := json.Marshal(ackInfo)
	if errMarshal == nil {
		redisKey := fmt.Sprintf("ackinfo:%s", message.ID)
		// Use SetEX for automatic expiration
		errSet := s.redisClient.SetEX(s.ctx, redisKey, string(ackInfoJSON), ackInfoTTL).Err()
		if errSet != nil {
			log.Printf("WARN: Failed to set temporary ack info in Redis for msg %s: %v", message.ID, errSet)
			// Continue routing even if Redis fails, ack notification might be delayed if lookup fails later
		}
	} else {
		log.Printf("ERROR: Failed to marshal AckInfo for msg %s: %v", message.ID, errMarshal)
		// Continue routing, ack notification might be delayed
	}
	// *** End Store Ack Info ***

	// Check if recipient is connected locally
	s.connectionsMux.RLock()
	recipient, ok := s.connections[recipientID]
	s.connectionsMux.RUnlock()

	if ok && recipient.IsConnected {
		// Recipient is local
		messageJSON, _ := json.Marshal(recipientCopy) // Marshal the 'sent' copy
		select {
		case recipient.Send <- messageJSON:
			log.Printf("Routed direct message %s to local recipient %s", message.ID, recipientID)
			// Server no longer sends immediate 'delivered' ack here.
			// Recipient client is now responsible for sending 'delivered' ack.
		default:
			log.Printf("WARN: Send channel full for recipient %s, dropping message %s", recipientID, message.ID)
			// If send fails even locally, store as pending
			s.storePendingMessage(message)
		}
	} else {
		// Recipient not local, check Redis for their instance
		instanceID, err := s.redisClient.Get(s.ctx, fmt.Sprintf("user:%s:instance", recipientID)).Result()
		if err == nil && instanceID != "" && instanceID != s.instanceID {
			// Recipient is on another known instance, forward via Redis Pub/Sub
			messageJSON, _ := json.Marshal(recipientCopy) // Marshal the 'sent' copy
			forwardCmd, _ := json.Marshal(map[string]interface{}{
				"type":    "forward_message",
				"to":      recipientID,
				"payload": string(messageJSON),
			})
			errPub := s.redisClient.Publish(s.ctx, fmt.Sprintf("instance:%s", instanceID), forwardCmd).Err()
			if errPub != nil {
				log.Printf("ERROR publishing forward_message for %s to instance %s: %v", recipientID, instanceID, errPub)
				// Store original message as pending if forwarding fails
				s.storePendingMessage(message)
			} else {
				log.Printf("Forwarded direct message %s via Redis to recipient %s on instance %s", message.ID, recipientID, instanceID)
			}
		} else {
			// Handle Redis error or user truly offline/instance unknown
			if err != nil && !errors.Is(err, redis.Nil) {
				log.Printf("ERROR checking Redis instance for recipient %s: %v", recipientID, err)
			}
			// Store the original 'sent' version as pending if offline or instance lookup fails
			s.storePendingMessage(message)
		}
	}

	// Add original 'sent' message to batch for DB insertion AFTER routing attempt and storing temp ack info
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

// *** MODIFIED sendMessageAck Signature and Logic ***
func (s *Server) sendMessageAck(messageID, ackingUserID, originalSenderID, originalChatID, status string) {
	// Prevent sending ack to self
	if originalSenderID == ackingUserID {
		// log.Printf("INFO: Suppressed sending self-ack for message %s by user %s", messageID, ackingUserID)
		return
	}
	// If original sender is empty (can happen from processAcknowledgment if DB lookup failed), log warning and stop.
	if originalSenderID == "" {
		log.Printf("WARN: sendMessageAck called with empty originalSenderID for message %s, cannot send notification.", messageID)
		return
	}
	if originalChatID == "" {
		log.Printf("WARN: sendMessageAck called with empty originalChatID for message %s, cannot send notification.", messageID)
		return
	}

	// Create notification payload (DB lookup removed)
	ackPayload := MessageAckPayload{
		MessageID: messageID,
		UserID:    ackingUserID, // Who performed the ack
		Status:    status,
		ChatID:    originalChatID, // Use passed-in chat ID
	}
	notification := CreateNotification("message_ack", ackPayload)
	notificationJSON, _ := json.Marshal(notification) // Error handled in CreateNotification

	// Route notification back to the original sender
	s.connectionsMux.RLock()
	senderClient, senderOnline := s.connections[originalSenderID]
	s.connectionsMux.RUnlock()

	if senderOnline && senderClient.IsConnected {
		// Sender connected locally
		senderClient.removeUnacknowledgedMessage(messageID) // Remove from internal tracking
		select {
		case senderClient.Send <- notificationJSON:
			// log.Printf("Sent '%s' ack notification for msg %s (chat %s) to local sender %s", status, messageID, originalChatID, originalSenderID)
		default:
			log.Printf("WARN: Send channel full for sender %s (Conn %s) for ack notification", originalSenderID, senderClient.ConnectionID)
		}
	} else {
		// Sender not local, forward via Redis
		instanceID, err := s.redisClient.Get(s.ctx, fmt.Sprintf("user:%s:instance", originalSenderID)).Result()
		if err == nil && instanceID != "" && instanceID != s.instanceID {
			forwardCmd, _ := json.Marshal(map[string]interface{}{
				"type":    "forward_notification",
				"to":      originalSenderID,
				"payload": string(notificationJSON),
			})
			errPub := s.redisClient.Publish(s.ctx, fmt.Sprintf("instance:%s", instanceID), forwardCmd).Err()
			if errPub != nil {
				log.Printf("ERROR publishing forward_notification (ack) sender %s instance %s: %v", originalSenderID, instanceID, errPub)
			}
			// else { log.Printf("Forwarded '%s' ack notification msg %s (chat %s) via Redis sender %s instance %s", status, messageID, originalChatID, originalSenderID, instanceID) }
		} else {
			// Log error if Redis lookup failed (excluding redis.Nil)
			if err != nil && !errors.Is(err, redis.Nil) {
				log.Printf("ERROR looking up instance for sender %s during ack forward: %v", originalSenderID, err)
			} else {
				log.Printf("Sender %s offline/instance unknown for ack notification %s", originalSenderID, messageID)
			}
			// Consider storing pending notifications if needed, but acks might be lower priority
		}
	}
}

func (s *Server) processAcknowledgment(messageID, ackingUserID, status string) {
	if status != "delivered" && status != "read" {
		log.Printf("ERROR: Invalid status '%s' from external ack msg %s", status, messageID)
		return
	}

	messagesCollection := s.mongoClient.Database(s.dbName).Collection("messages")
	var originalMessage MessagePacket
	var ackInfoFromRedis AckInfo
	var foundInDB = false
	var fetchedSender = ""
	var fetchedChatID = ""

	// 1. Attempt to find message in DB
	findErr := messagesCollection.FindOne(s.ctx, bson.M{"id": messageID}).Decode(&originalMessage)

	if findErr == nil {
		foundInDB = true
		fetchedSender = originalMessage.From
		fetchedChatID = originalMessage.To
		log.Printf("Found message %s in DB for ack processing.", messageID)
	} else if errors.Is(findErr, mongo.ErrNoDocuments) {
		log.Printf("WARN: External ack received for message %s, but message not found in DB yet. Attempting Redis lookup.", messageID)
		// Attempt Redis lookup if not found in DB
		redisKey := fmt.Sprintf("ackinfo:%s", messageID)
		ackInfoJSON, redisErr := s.redisClient.Get(s.ctx, redisKey).Result()
		if redisErr == nil {
			if errUnmarshal := json.Unmarshal([]byte(ackInfoJSON), &ackInfoFromRedis); errUnmarshal == nil {
				fetchedSender = ackInfoFromRedis.Sender
				fetchedChatID = ackInfoFromRedis.ChatID
				log.Printf("Found ack info for message %s in Redis.", messageID)
			} else {
				log.Printf("ERROR unmarshaling ack info from Redis for msg %s: %v", messageID, errUnmarshal)
			}
		} else if !errors.Is(redisErr, redis.Nil) {
			log.Printf("ERROR looking up ack info in Redis for msg %s: %v", messageID, redisErr)
		} else {
			log.Printf("WARN: Ack info for message %s not found in Redis either (likely expired or never set).", messageID)
		}
	} else {
		// Other DB error finding message
		log.Printf("ERROR finding original message %s for external ack processing: %v", messageID, findErr)
	}

	// 2. Attempt to update DB status IF message was found there initially
	if foundInDB {
		update := bson.M{"$set": bson.M{"status": status}}
		now := time.Now()
		if status == "delivered" {
			update["$set"].(bson.M)["delivered_at"] = now
		}
		if status == "read" {
			update["$set"].(bson.M)["read_at"] = now
			update["$set"].(bson.M)["delivered_at"] = now
		} // Assume delivered if read

		result, updateErr := messagesCollection.UpdateOne(s.ctx, bson.M{"id": messageID}, update)

		// --- FIX: Added curly braces to the 'if' block ---
		if updateErr != nil {
			log.Printf("ERROR updating message %s status to '%s' via external ack: %v", messageID, status, updateErr)
		} else if result.MatchedCount > 0 && result.ModifiedCount > 0 {
			log.Printf("Updated status msg %s to '%s' via external ack from %s (DB)", messageID, status, ackingUserID)
		} else if result.MatchedCount > 0 { // Use plain 'else if' here now
			log.Printf("INFO: Status update for msg %s to '%s' from %s resulted in no DB modification.", messageID, status, ackingUserID)
		}
		// --- End Fix ---

	} else {
		// If not found in DB initially, log that update is skipped for now
		log.Printf("Skipping DB status update for msg %s (not found in initial lookup). Will be updated if message arrives later.", messageID)
	}

	// 3. Send notification back to the original sender IF we have sender/chatID details (from DB or Redis)
	if fetchedSender != "" && fetchedChatID != "" {
		s.sendMessageAck(messageID, ackingUserID, fetchedSender, fetchedChatID, status)
		// Clean up Redis key if we successfully sent notification based on Redis data
		if !foundInDB {
			redisKey := fmt.Sprintf("ackinfo:%s", messageID)
			_, delErr := s.redisClient.Del(s.ctx, redisKey).Result()
			if delErr != nil {
				log.Printf("WARN: Failed to delete ackinfo key %s from Redis after use: %v", redisKey, delErr)
			}
		}
	} else {
		log.Printf("Skipping send ack notification for msg %s because sender/chat details could not be determined.", messageID)
	}
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

// server.go

// ... other imports and code ...

func (s *Server) routeGroupMessage(message MessagePacket) {
	groupID := message.To

	// *** Store Ack Info in Redis before batching/routing ***
	ackInfo := AckInfo{Sender: message.From, ChatID: groupID} // ChatID is groupID here
	ackInfoJSON, errMarshal := json.Marshal(ackInfo)
	if errMarshal == nil {
		redisKey := fmt.Sprintf("ackinfo:%s", message.ID)
		errSet := s.redisClient.SetEX(s.ctx, redisKey, string(ackInfoJSON), ackInfoTTL).Err()
		if errSet != nil {
			log.Printf("WARN: Failed to set temporary ack info in Redis for group msg %s: %v", message.ID, errSet)
		}
	} else {
		log.Printf("ERROR: Failed to marshal AckInfo for group msg %s: %v", message.ID, errMarshal)
	}
	// *** End Store Ack Info ***

	// Fetch group metadata
	collection := s.mongoClient.Database(s.dbName).Collection("groups")
	var group GroupMetadata
	err := collection.FindOne(s.ctx, bson.M{"_id": groupID}).Decode(&group)
	if err != nil {
		log.Printf("Error fetching group %s for routing message %s: %v", groupID, message.ID, err)
		// Optionally send error back to sender? For now, just log and stop routing.
		return
	}

	// Distribute message to members
	membersOnThisInstance := []string{}
	membersByInstance := make(map[string][]string)
	offlineMembers := []string{}

	for _, memberID := range group.Members {
		// Don't send message back to the original sender
		if memberID == message.From {
			continue
		}

		// Check local connections
		s.connectionsMux.RLock()
		member, ok := s.connections[memberID]
		s.connectionsMux.RUnlock()

		if ok && member.IsConnected {
			membersOnThisInstance = append(membersOnThisInstance, memberID)
		} else {
			// Check Redis for other instances
			instanceID, err := s.redisClient.Get(s.ctx, fmt.Sprintf("user:%s:instance", memberID)).Result()
			if err == nil && instanceID != "" && instanceID != s.instanceID {
				membersByInstance[instanceID] = append(membersByInstance[instanceID], memberID)
			} else {
				// User is offline or Redis lookup failed
				if err != nil && !errors.Is(err, redis.Nil) {
					log.Printf("WARN: Redis error checking instance for group member %s: %v", memberID, err)
				}
				offlineMembers = append(offlineMembers, memberID)
			}
		}
	}

	// Prepare the message copy to send (with 'sent' status)
	recipientCopy := message // Status is already 'sent' from handleClientGroupMessage
	messageJSON, errMarshalMsg := json.Marshal(recipientCopy)
	if errMarshalMsg != nil {
		log.Printf("CRITICAL: Failed to marshal group message %s for routing: %v", message.ID, errMarshalMsg)
		return // Cannot proceed if message can't be marshaled
	}

	// Send to local members
	for _, memberID := range membersOnThisInstance {
		s.connectionsMux.RLock()
		member, ok := s.connections[memberID]
		s.connectionsMux.RUnlock()
		if ok && member.IsConnected {
			select {
			case member.Send <- messageJSON:
				log.Printf("Routed group message %s to local member %s", message.ID, memberID)
				// No immediate 'delivered' ack from server
			default:
				log.Printf("WARN: Send chan full for local group member %s (Conn %s), dropping group msg %s", memberID, member.ConnectionID, message.ID)
				// Consider storing pending if local send fails?
				// offlineMembers = append(offlineMembers, memberID) // Add to offline list if send fails?
			}
		}
	}

	// Forward to other instances via Redis
	for instanceID, members := range membersByInstance {
		forwardCmd, _ := json.Marshal(map[string]interface{}{
			"type":     "forward_group_message",
			"group_id": groupID,
			"members":  members,             // List of members on that specific instance
			"payload":  string(messageJSON), // The message to forward
		})
		errPub := s.redisClient.Publish(s.ctx, fmt.Sprintf("instance:%s", instanceID), forwardCmd).Err()
		if errPub != nil {
			log.Printf("ERROR publishing forward_group_message for group %s to instance %s: %v", groupID, instanceID, errPub)
			// Add these members to offline list if forwarding fails?
			// offlineMembers = append(offlineMembers, members...)
		} else {
			log.Printf("Forwarded group message %s via Redis to %d members on instance %s", message.ID, len(members), instanceID)
		}
	}

	// Store pending for offline members
	// Note: Current logic stores one pending message per offline member, targeted at them.
	// An alternative is storing one message per group, targeted at the groupID,
	// and handling delivery upon member connection (more complex).
	for _, memberID := range offlineMembers {
		pendingMsg := message    // Store the original 'sent' message
		pendingMsg.To = memberID // Target the specific offline member
		s.storePendingMessage(pendingMsg)
	}

	// Add original 'sent' message to batch for DB insertion AFTER routing/storing ack info
	s.addToBatch("messages", message)

	// Update group's last message time in DB
	_, updateErr := collection.UpdateOne(s.ctx, bson.M{"_id": groupID}, bson.M{"$set": bson.M{"last_message": message.Timestamp}})
	if updateErr != nil {
		log.Printf("WARN: Failed update group %s last_message time: %v", groupID, updateErr)
	}
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

// This function handles when a client explicitly sends a message_ack (e.g., for 'read')
func (s *Server) handleMessageAck(client *Client, messageID, status string) {
	// Use processAcknowledgment which handles DB update and notifying the sender
	// It needs the acking user (client.UserID) and the status from the client message
	s.processAcknowledgment(messageID, client.UserID, status)

	// The `removeUnacknowledgedMessage` for the *sender* is now handled within `sendMessageAck`
	// when the notification is successfully routed (locally or via Redis).
	// No need to remove it here for the acking client unless tracking received unacked messages.

	log.Printf("Processed client '%s' ack for message %s from user %s", status, messageID, client.UserID)
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
	if limit <= 0 || limit > 100 {
		limit = 30
	} // Apply default/max limit

	filter := bson.M{}
	// Build filter based on type
	if isGroup {
		filter["type"] = "group"
		filter["to"] = chatID // Group ID is in the 'to' field for group messages
	} else {
		// Direct message filter
		filter["type"] = "direct"
		filter["$or"] = []bson.M{
			{"from": client.UserID, "to": chatID},
			{"from": chatID, "to": client.UserID},
		}
	}

	// Apply 'before' timestamp filter if provided (expecting milliseconds from frontend)
	if before > 0 {
		// Convert milliseconds timestamp to time.Time
		beforeTime := time.Unix(0, before*int64(time.Millisecond))
		filter["timestamp"] = bson.M{"$lt": beforeTime}
		log.Printf("Fetching history for %s (group: %v) before %v", chatID, isGroup, beforeTime)
	} else {
		log.Printf("Fetching initial history for %s (group: %v)", chatID, isGroup)
	}

	collection := s.mongoClient.Database(s.dbName).Collection("messages")

	// Fetch limit + 1 to check if more exist. Sort descending (newest first for the page).
	fetchLimit := int64(limit + 1)
	opts := options.Find().SetSort(bson.D{{Key: "timestamp", Value: -1}}).SetLimit(fetchLimit)

	cursor, err := collection.Find(s.ctx, filter, opts)
	if err != nil {
		log.Printf("ERROR fetching history for chat %s: %v", chatID, err)
		// Send error notification back to the client
		errorPayload := ErrorPayload{Message: "Failed to fetch chat history."}
		errorNotif := CreateNotification("error", errorPayload)
		errorJSON, _ := json.Marshal(errorNotif)
		select {
		case client.Send <- errorJSON:
		default:
		} // Non-blocking send
		return
	}
	defer cursor.Close(s.ctx)

	var messagesFound []MessagePacket
	if err = cursor.All(s.ctx, &messagesFound); err != nil {
		log.Printf("ERROR decoding history messages for chat %s: %v", chatID, err)
		// Send error notification back to the client
		errorPayload := ErrorPayload{Message: "Failed to decode chat history."}
		errorNotif := CreateNotification("error", errorPayload)
		errorJSON, _ := json.Marshal(errorNotif)
		select {
		case client.Send <- errorJSON:
		default:
		}
		return
	}

	// Determine if there are more older messages than requested
	hasMore := len(messagesFound) > limit
	messagesToSend := messagesFound
	if hasMore {
		// Remove the extra message used only for the 'hasMore' check
		messagesToSend = messagesFound[:limit]
	}

	// Send response back to the *requesting* client
	historyResponsePayload := HistoryResponsePayload{
		ChatID:   chatID,
		Messages: messagesToSend, // Messages are newest first within the fetched page
		IsGroup:  isGroup,
		HasMore:  hasMore, // Inform client if more exist
	}
	notification := CreateNotification("history_response", historyResponsePayload)
	notificationJSON, _ := json.Marshal(notification)

	select {
	case client.Send <- notificationJSON:
		log.Printf("Sent history response (%d messages, hasMore: %v) for chat %s to %s", len(messagesToSend), hasMore, chatID, client.UserID)
	default:
		log.Printf("WARN: Send channel full for user %s (Conn %s) when sending history response", client.UserID, client.ConnectionID)
	}
}

func (s *Server) handleVersion(w http.ResponseWriter, r *http.Request) {
	version := map[string]string{
		"version":    "1.0.0", // Change this for each test (e.g., "1.0.1")
		"build_time": time.Now().Format(time.RFC3339),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(version)
}

func (s *Server) handleLoadGroups(client *Client) {
	userGroupsCollection := s.mongoClient.Database(s.dbName).Collection("user_groups")
	groupsCollection := s.mongoClient.Database(s.dbName).Collection("groups")

	log.Printf("Handling load_groups request for user %s", client.UserID)

	// 1. Find the user's group memberships document
	var userGroupsDoc struct {
		UserID string   `bson:"user_id"`
		Groups []string `bson:"groups"` // Assuming group IDs are stored as strings
	}
	err := userGroupsCollection.FindOne(s.ctx, bson.M{"user_id": client.UserID}).Decode(&userGroupsDoc)

	// Handle cases where user has no groups or document doesn't exist
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			log.Printf("User %s has no group memberships document or is not in any groups.", client.UserID)
			// Send an empty list back
			groupsListPayload := GroupsListPayload{Groups: []GroupMetadata{}}
			notification := CreateNotification("groups_list", groupsListPayload)
			notificationJSON, _ := json.Marshal(notification)
			select {
			case client.Send <- notificationJSON:
			default:
			}
			return
		}
		// Handle other potential errors during fetch
		log.Printf("ERROR finding user_groups document for %s: %v", client.UserID, err)
		errorPayload := ErrorPayload{Message: "Failed to retrieve your group memberships."}
		errorNotif := CreateNotification("error", errorPayload)
		errorJSON, _ := json.Marshal(errorNotif)
		select {
		case client.Send <- errorJSON:
		default:
		}
		return
	}

	// Check if the groups list is empty
	if len(userGroupsDoc.Groups) == 0 {
		log.Printf("User %s is member of 0 groups.", client.UserID)
		groupsListPayload := GroupsListPayload{Groups: []GroupMetadata{}}
		notification := CreateNotification("groups_list", groupsListPayload)
		notificationJSON, _ := json.Marshal(notification)
		select {
		case client.Send <- notificationJSON:
		default:
		}
		return
	}

	// 2. Fetch metadata for the groups the user is a member of
	// Assuming Group IDs are stored and queried as strings in the 'groups' collection _id field
	filter := bson.M{"_id": bson.M{"$in": userGroupsDoc.Groups}}
	// Add projection if you only need specific fields (id, name, etc.)
	// opts := options.Find().SetProjection(bson.M{"_id": 1, "name": 1, "last_message": 1}) // Example projection

	cursor, err := groupsCollection.Find(s.ctx, filter /*, opts*/) // Add opts if using projection
	if err != nil {
		log.Printf("ERROR fetching group metadata for user %s (%d groups): %v", client.UserID, len(userGroupsDoc.Groups), err)
		errorPayload := ErrorPayload{Message: "Failed to retrieve details for your groups."}
		errorNotif := CreateNotification("error", errorPayload)
		errorJSON, _ := json.Marshal(errorNotif)
		select {
		case client.Send <- errorJSON:
		default:
		}
		return
	}
	defer cursor.Close(s.ctx)

	var groups []GroupMetadata
	if err = cursor.All(s.ctx, &groups); err != nil {
		log.Printf("ERROR decoding group metadata for user %s: %v", client.UserID, err)
		errorPayload := ErrorPayload{Message: "Failed to process group details."}
		errorNotif := CreateNotification("error", errorPayload)
		errorJSON, _ := json.Marshal(errorNotif)
		select {
		case client.Send <- errorJSON:
		default:
		}
		return
	}

	// 3. Send the list back to the *requesting* client
	groupsListPayload := GroupsListPayload{Groups: groups}
	notification := CreateNotification("groups_list", groupsListPayload)
	notificationJSON, _ := json.Marshal(notification)

	select {
	case client.Send <- notificationJSON:
		log.Printf("Sent groups list (%d groups) to user %s", len(groups), client.UserID)
	default:
		log.Printf("WARN: Send channel full for user %s (Conn %s) when sending groups list", client.UserID, client.ConnectionID)
	}
}
