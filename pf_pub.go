package pf_pub

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
)

const (
	projectID string = "d291209"
	topicID   string = "pubsub_test"
)

// User for contract on schema user
type User struct {
	UserID    int       `json:"user_id"`
	Name      string    `json:"name"`
	Gender    string    `json:"gender"`
	CreatedAt time.Time `json:"created_at"`
}

// PublishUser for publishing user to pubsub
func PublishUser(u User) error {
	message, _ := json.Marshal(u)
	err := publish(projectID, topicID, string(message))
	return err
}

func publish(projectID, topicID, msg string) error {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("pubsub.NewClient: %v", err)
	}

	t := client.Topic(topicID)
	result := t.Publish(ctx, &pubsub.Message{
		Data: []byte(msg),
	})
	// Block until the result is returned and a server-generated
	// ID is returned for the published message.
	id, err := result.Get(ctx)
	if err != nil {
		return fmt.Errorf("Get: %v", err)
	}
	fmt.Printf("Published a message; msg ID: %v\n", id)
	return nil
}
