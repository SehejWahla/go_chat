package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
)

type MessagePacket struct {
	ID          string          `json:"id" bson:"id"`
	Type        string          `json:"type" bson:"type"`
	Action      string          `json:"action,omitempty" bson:"action,omitempty"`
	From        string          `json:"from" bson:"from"`
	To          string          `json:"to" bson:"to"`
	Content     json.RawMessage `json:"content" bson:"content"`
	Timestamp   time.Time       `json:"timestamp" bson:"timestamp"`
	Status      string          `json:"status,omitempty" bson:"status,omitempty"`
	DeliveredAt time.Time       `json:"delivered_at,omitempty" bson:"delivered_at,omitempty"`
	ReadAt      time.Time       `json:"read_at,omitempty" bson:"read_at,omitempty"`
	ClientMsgID string          `json:"client_msg_id,omitempty" bson:"client_msg_id,omitempty"`
	SequenceNum int64           `json:"seq_num,omitempty" bson:"seq_num,omitempty"`
}

type ChatEvent struct {
	ID        string          `json:"id" bson:"id"`
	Type      string          `json:"type" bson:"type"`
	UserID    string          `json:"user_id" bson:"user_id"`
	Data      json.RawMessage `json:"data" bson:"data"`
	Timestamp time.Time       `json:"timestamp" bson:"timestamp"`
}

type GroupMetadata struct {
	ID          string    `json:"id" bson:"_id"`
	Name        string    `json:"name" bson:"name"`
	Members     []string  `json:"members" bson:"members"`
	CreatedAt   time.Time `json:"created_at" bson:"created_at"`
	CreatedBy   string    `json:"created_by" bson:"created_by"`
	LastMessage time.Time `json:"last_message" bson:"last_message"`
}

type UserConnection struct {
	UserID        string    `json:"user_id" bson:"user_id"`
	ConnectionID  string    `json:"connection_id" bson:"connection_id"`
	InstanceID    string    `json:"instance_id" bson:"instance_id"`
	Connected     bool      `json:"connected" bson:"connected"`
	LastSeen      time.Time `json:"last_seen" bson:"last_seen"`
	LastSequence  int64     `json:"last_sequence" bson:"last_sequence"`
	Device        string    `json:"device" bson:"device"`
	ClientVersion string    `json:"client_version" bson:"client_version"`
}

type MessageAck struct {
	UserID    string    `json:"user_id"`
	MessageID string    `json:"message_id"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

// Payload for 'message_ack' notification content
type MessageAckPayload struct {
	MessageID string `json:"message_id"` // ID of the message being acknowledged
	UserID    string `json:"user_id"`    // User who triggered the ack (the recipient)
	Status    string `json:"status"`     // The new status ("delivered", "read")
	ChatID    string `json:"chat_id"`    // **ADDED**: The original 'to' field (recipient/group ID) of the acknowledged message
}

// Payload for 'history_response' notification content
type HistoryResponsePayload struct {
	ChatID   string          `json:"chat_id"`  // User email or Group ID
	Messages []MessagePacket `json:"messages"` // Array of messages (sorted newest first for the page)
	IsGroup  bool            `json:"is_group"` // Was this history for a group?
	HasMore  bool            `json:"has_more"` // **ADDED**: Are there more older messages available?
}

// Payload for 'groups_list' notification content (NEW)
type GroupsListPayload struct {
	Groups []GroupMetadata `json:"groups"` // List of groups the user is in
}

// Payload for error notification content (NEW)
type ErrorPayload struct {
	Message string `json:"message"`
	Details string `json:"details,omitempty"`
}

func CreateNotification(action string, data interface{}) MessagePacket {
	content, err := json.Marshal(data)
	originalAction := action // Store original action in case we change it to 'error'
	if err != nil {
		log.Printf("ERROR marshaling notification data for action %s: %v. Data: %+v", originalAction, err, data)
		// Create a fallback error content
		errorPayload := ErrorPayload{
			Message: fmt.Sprintf("Internal server error preparing '%s' notification.", originalAction),
			Details: err.Error(),
		}
		content, _ = json.Marshal(errorPayload) // Marshal the error payload
		action = "error"                        // Change action type to error
	}
	return MessagePacket{
		ID:        NewUUID(),
		Type:      "notification",
		Action:    action, // Use potentially updated action ('error')
		Content:   content,
		Timestamp: time.Now(),
		Status:    "sent", // Status might not be relevant for all notifications
	}
}

func NewUUID() string {
	return uuid.New().String()
}
