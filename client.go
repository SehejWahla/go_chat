package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 10240
)

type Client struct {
	UserID             string
	ConnectionID       string
	FirstName          string
	Connection         *websocket.Conn
	Server             *Server
	Send               chan []byte
	Groups             []string
	LastSeen           time.Time
	IsConnected        bool
	lastSequenceNumber int64
	unackedMessages    map[string]MessagePacket
	msgMutex           sync.RWMutex
	connInfo           UserConnection
	Token              string
}

func NewClient(userID string, conn *websocket.Conn, server *Server, token string) *Client {
	connectionID := uuid.New().String()
	client := &Client{
		UserID:          userID,
		ConnectionID:    connectionID,
		Connection:      conn,
		Server:          server,
		Send:            make(chan []byte, 512), // Use a reasonable buffer
		Groups:          []string{},
		LastSeen:        time.Now(),
		IsConnected:     true,
		unackedMessages: make(map[string]MessagePacket),
		Token:           token,
	}
	client.connInfo = UserConnection{
		UserID:       userID,
		ConnectionID: connectionID,
		InstanceID:   server.instanceID,
		Connected:    true,
		LastSeen:     client.LastSeen,
	}
	return client
}

func (c *Client) readPump() {
	// Cleanup resources when the readPump exits (due to error or connection close)
	defer func() {
		c.Server.unregisterClient(c) // Remove client from server's map and update Redis
		c.Connection.Close()         // Ensure WebSocket connection is closed
		log.Printf("Closed readPump and unregistered client %s (%s)", c.UserID, c.ConnectionID)
	}()

	// Configure the underlying connection
	c.Connection.SetReadLimit(maxMessageSize)                  // Set max message size
	_ = c.Connection.SetReadDeadline(time.Now().Add(pongWait)) // Set initial read deadline
	// Pong handler resets the read deadline
	c.Connection.SetPongHandler(func(string) error {
		// log.Printf("Pong received from %s", c.UserID) // Debug log
		_ = c.Connection.SetReadDeadline(time.Now().Add(pongWait))
		c.LastSeen = time.Now() // Update activity time
		// Optionally update Redis less frequently if needed
		// c.updateConnectionInfo()
		return nil
	})

	// Loop indefinitely, reading messages from the WebSocket
	for {
		messageType, messageBytes, err := c.Connection.ReadMessage()
		if err != nil {
			// Check for specific close errors vs other errors
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure, websocket.CloseNormalClosure) {
				// Log unexpected errors more verbosely
				log.Printf("WebSocket unexpected read error for %s (%s): %v", c.UserID, c.ConnectionID, err)
			} else if errors.Is(err, net.ErrClosed) {
				// Connection closed by local operation (e.g., unregisterClient)
				log.Printf("WebSocket connection closed by network operation for %s (%s)", c.UserID, c.ConnectionID)
			} else {
				// Log normal closure or other errors less verbosely
				log.Printf("WebSocket read loop exiting for %s (%s). Error: %v", c.UserID, c.ConnectionID, err)
			}
			break // Exit the read loop on any error
		}

		// Ignore non-text messages
		if messageType != websocket.TextMessage {
			log.Printf("Received non-text message type: %d from %s. Ignoring.", messageType, c.UserID)
			continue
		}

		// --- Parse Base Packet to get Action ---
		var basePacket struct {
			Action  string          `json:"action"`
			Content json.RawMessage `json:"content"` // Keep content raw for action-specific parsing
		}
		if err := json.Unmarshal(messageBytes, &basePacket); err != nil {
			log.Printf("ERROR parsing base packet from %s: %v. Raw: %s", c.UserID, err, string(messageBytes))
			// Send error notification back to client
			errorPayload := ErrorPayload{Message: "Invalid message format received."}
			errorNotif := CreateNotification("error", errorPayload)
			errorJSON, _ := json.Marshal(errorNotif)
			select {
			case c.Send <- errorJSON:
			default:
			} // Non-blocking send
			continue // Skip processing this invalid message
		}

		// --- Update Activity Time ---
		c.LastSeen = time.Now()

		// --- Delegate Action Handling ---
		if err := c.handleWebSocketAction(basePacket.Action, messageBytes, basePacket.Content); err != nil {
			log.Printf("ERROR handling action '%s' from %s: %v", basePacket.Action, c.UserID, err)
			// Send error notification back to client
			errorPayload := ErrorPayload{
				Message: fmt.Sprintf("Failed to process action '%s'.", basePacket.Action),
				Details: err.Error(), // Include specific error detail
			}
			errorNotif := CreateNotification("error", errorPayload)
			errorJSON, _ := json.Marshal(errorNotif)
			select {
			case c.Send <- errorJSON:
			default:
			}
		}

		// Pong handler already resets deadline, manually resetting here might be redundant
		// _ = c.Connection.SetReadDeadline(time.Now().Add(pongWait))
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		c.Connection.Close()
	}()

	for {
		select {
		case message, ok := <-c.Send:
			c.Connection.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.Connection.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			w, err := c.Connection.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)
			n := len(c.Send)
			for i := 0; i < n; i++ {
				w.Write([]byte{'\n'})
				w.Write(<-c.Send)
			}
			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			c.Connection.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.Connection.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
			c.LastSeen = time.Now()
			c.updateConnectionInfo()
		}
	}
}

func (c *Client) handleWebSocketAction(action string, rawMessage []byte, rawContent json.RawMessage) error {
	log.Printf("Handling action '%s' for user %s", action, c.UserID) // Log action being handled

	switch action {
	case "direct_message", "group_message":
		var msgData struct {
			To          string `json:"to"`                      // Expect 'to' at the top level
			ClientMsgID string `json:"client_msg_id,omitempty"` // Optional client ID
		}
		// Unmarshal relevant fields from the raw message (not just content)
		if err := json.Unmarshal(rawMessage, &msgData); err != nil {
			return fmt.Errorf("parsing '%s' data: %w", action, err)
		}
		if msgData.To == "" {
			return fmt.Errorf("missing 'to' field for '%s'", action)
		}
		// Content is passed as raw JSON from basePacket
		if action == "direct_message" {
			c.Server.handleClientDirectMessage(c, msgData.To, rawContent, msgData.ClientMsgID)
		} else { // group_message
			c.Server.handleClientGroupMessage(c, msgData.To, rawContent, msgData.ClientMsgID)
		}

	case "create_group":
		var groupData struct {
			Name    string   `json:"name"` // Expect these fields within 'content'
			Members []string `json:"members"`
		}
		// Unmarshal specific fields from rawContent
		if err := json.Unmarshal(rawContent, &groupData); err != nil {
			return fmt.Errorf("parsing create_group content: %w", err)
		}
		if groupData.Name == "" {
			return errors.New("missing 'name' in create_group content")
		}
		// Ensure creator is included in members list (idempotent)
		if !contains(groupData.Members, c.UserID) {
			groupData.Members = append(groupData.Members, c.UserID)
		}
		// Add default member if list is empty? Or require members? Assuming required.
		if len(groupData.Members) <= 1 && !contains(groupData.Members, c.UserID) { // Check if only creator after adding self
			// Maybe require at least one other member? Or allow self-groups?
			// return errors.New("group must have at least one other member besides creator")
		}
		c.Server.handleCreateGroup(c, groupData.Name, groupData.Members)

	case "message_ack":
		var ackData struct {
			AckMessageID string `json:"ack_message_id"` // Expect top-level fields
			AckStatus    string `json:"ack_status"`
		}
		// Unmarshal from the raw message
		if err := json.Unmarshal(rawMessage, &ackData); err != nil {
			return fmt.Errorf("parsing message_ack data: %w", err)
		}
		if ackData.AckMessageID == "" || (ackData.AckStatus != "delivered" && ackData.AckStatus != "read") {
			return fmt.Errorf("missing or invalid fields for message_ack (id: '%s', status: '%s')", ackData.AckMessageID, ackData.AckStatus)
		}
		c.Server.handleMessageAck(c, ackData.AckMessageID, ackData.AckStatus)

	case "load_history":
		var historyRequest struct {
			ChatID  string `json:"chat_id"`
			Before  int64  `json:"before,omitempty"` // Unix Milliseconds Timestamp
			Limit   int    `json:"limit,omitempty"`
			IsGroup bool   `json:"is_group"`
		}
		// Unmarshal specific fields from rawContent
		if err := json.Unmarshal(rawContent, &historyRequest); err != nil {
			return fmt.Errorf("parsing load_history content: %w", err)
		}
		if historyRequest.ChatID == "" {
			return errors.New("missing 'chat_id' in load_history content")
		}
		c.Server.handleHistoryRequest(c, historyRequest.ChatID, historyRequest.Before, historyRequest.Limit, historyRequest.IsGroup)

	case "load_groups":
		// This action might not need any specific content payload
		c.Server.handleLoadGroups(c) // Call the server handler

	case "typing":
		var typingData struct {
			To string `json:"to"` // Expect 'to' at the top level
		}
		// Unmarshal from the raw message
		if err := json.Unmarshal(rawMessage, &typingData); err != nil {
			return fmt.Errorf("parsing typing data: %w", err)
		}
		if typingData.To == "" {
			return errors.New("missing 'to' field for typing")
		}
		c.Server.handleTypingIndicator(c, typingData.To)

	case "check_online_status":
		var checkRequest struct {
			UserID string `json:"user_id"` // Expect user_id within 'content'
		}
		// Unmarshal specific fields from rawContent
		if err := json.Unmarshal(rawContent, &checkRequest); err != nil {
			return fmt.Errorf("parsing check_online_status content: %w", err)
		}
		if checkRequest.UserID == "" {
			return errors.New("missing 'user_id' in check_online_status content")
		}
		c.Server.handleOnlineStatusCheck(c, checkRequest.UserID)

	default:
		log.Printf("WARN: Received unknown action '%s' from %s", action, c.UserID)
		// Optionally return an error to notify client
		// return fmt.Errorf("unknown action received: %s", action)
	}
	return nil // Indicate success (action recognized and processed or logged)
}

func (c *Client) updateConnectionInfo() {
	c.connInfo.LastSeen = c.LastSeen
	c.connInfo.Connected = c.IsConnected
	c.Server.updateConnectionInfo(c.connInfo)
}

func (c *Client) addUnacknowledgedMessage(msg MessagePacket) {
	c.msgMutex.Lock()
	defer c.msgMutex.Unlock()
	c.unackedMessages[msg.ID] = msg
}

func (c *Client) removeUnacknowledgedMessage(messageID string) {
	c.msgMutex.Lock()
	defer c.msgMutex.Unlock()
	delete(c.unackedMessages, messageID)
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
