package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

type Client struct {
	UserID             string
	ConnectionID       string
	Connection         *websocket.Conn
	Server             *Server
	Send               chan []byte
	Groups             []string
	LastSeen           time.Time
	IsConnected        bool
	DeviceInfo         string
	ClientVersion      string
	lastSequenceNumber int64
	unackedMessages    map[string]MessagePacket
	msgMutex           sync.RWMutex
	connInfo           UserConnection
}

func NewClient(userID string, conn *websocket.Conn, server *Server) *Client {
	connectionID := uuid.New().String()
	connInfo := UserConnection{
		UserID:       userID,
		ConnectionID: connectionID,
		InstanceID:   server.instanceID,
		Connected:    true,
		LastSeen:     time.Now(),
		LastSequence: 0,
	}
	return &Client{
		UserID:          userID,
		ConnectionID:    connectionID,
		Connection:      conn,
		Server:          server,
		Send:            make(chan []byte, 256),
		Groups:          []string{},
		LastSeen:        time.Now(),
		IsConnected:     true,
		unackedMessages: make(map[string]MessagePacket),
		msgMutex:        sync.RWMutex{},
		connInfo:        connInfo,
	}
}

func (c *Client) readPump() {
	defer func() {
		c.Server.unregisterClient(c)
		c.Connection.Close()
	}()

	c.Connection.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.Connection.SetPongHandler(func(string) error {
		c.Connection.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, message, err := c.Connection.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket read error: %v", err)
			}
			break
		}

		var packet struct {
			Action       string          `json:"action"`
			To           string          `json:"to"`
			Content      json.RawMessage `json:"content"`
			ClientMsgID  string          `json:"client_msg_id,omitempty"`
			AckMessageID string          `json:"ack_message_id,omitempty"`
			AckStatus    string          `json:"ack_status,omitempty"`
		}
		if err := json.Unmarshal(message, &packet); err != nil {
			log.Printf("Error parsing message: %v", err)
			continue
		}

		c.LastSeen = time.Now()
		c.updateConnectionInfo()

		switch packet.Action {
		case "direct_message":
			c.Server.handleClientDirectMessage(c, packet.To, packet.Content, packet.ClientMsgID)
		case "group_message":
			c.Server.handleClientGroupMessage(c, packet.To, packet.Content, packet.ClientMsgID)
		case "create_group":
			var groupData struct {
				Name    string   `json:"name"`
				Members []string `json:"members"`
			}
			if err := json.Unmarshal(packet.Content, &groupData); err != nil {
				log.Printf("Error parsing group data: %v", err)
				continue
			}
			if !contains(groupData.Members, c.UserID) {
				groupData.Members = append([]string{c.UserID}, groupData.Members...)
			}
			c.Server.handleCreateGroup(c, groupData.Name, groupData.Members)
		case "message_ack":
			if packet.AckMessageID != "" {
				c.Server.handleMessageAck(c, packet.AckMessageID, packet.AckStatus)
			}
		case "load_history":
			var historyRequest struct {
				ChatID  string `json:"chat_id"`
				Before  int64  `json:"before,omitempty"`
				Limit   int    `json:"limit,omitempty"`
				IsGroup bool   `json:"is_group"`
			}
			if err := json.Unmarshal(packet.Content, &historyRequest); err != nil {
				log.Printf("Error parsing history request: %v", err)
				continue
			}
			c.Server.handleHistoryRequest(c, historyRequest.ChatID, historyRequest.Before, historyRequest.Limit, historyRequest.IsGroup)
		case "presence":
			c.Server.updateUserPresence(c)
		case "typing":
			c.Server.handleTypingIndicator(c, packet.To)
		case "check_online_status":
			var checkRequest struct {
				UserID string `json:"user_id"`
			}
			if err := json.Unmarshal(packet.Content, &checkRequest); err != nil {
				log.Printf("Error parsing online status check: %v", err)
				continue
			}
			c.Server.handleOnlineStatusCheck(c, checkRequest.UserID)
		}
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
