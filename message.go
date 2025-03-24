package main

import (
	"encoding/json"
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

func CreateNotification(action string, data map[string]interface{}) MessagePacket {
	content, _ := json.Marshal(data)
	return MessagePacket{
		ID:        NewUUID(),
		Type:      "notification",
		Action:    action,
		Content:   content,
		Timestamp: time.Now(),
		Status:    "sent",
	}
}

func NewUUID() string {
	return uuid.New().String()
}
