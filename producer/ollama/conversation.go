package ollama

import (
	"context"
	"sync"
)

type Conversation struct {
	client  *Client
	model   string
	mu      sync.Mutex
	history []Message
}

func NewConversation(client *Client, model, systemPrompt string) *Conversation {
	history := []Message{}

	if systemPrompt != "" {
		history = append(history, Message{
			Role:    "system",
			Content: systemPrompt,
		})
	}

	return &Conversation{
		client:  client,
		model:   model,
		history: history,
	}
}

func (c *Conversation) Send(ctx context.Context, userInput string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.history = append(c.history, Message{
		Role:    "user",
		Content: userInput,
	})

	resp, err := c.client.Chat(ctx, ChatRequest{
		Model:    c.model,
		Messages: c.history,
	})
	if err != nil {
		return "", err
	}

	c.history = append(c.history, resp.Message)

	return resp.Message.Content, nil
}