package broker

import (
	"context"
	"errors"
	"time"
)

type IBroker interface {
	Publish(ctx context.Context, queue string, groupId *string, data *string) error
	Subscribe(ctx context.Context, conf Configuration, fn func(context.Context, IMessage)) error
}

type Configuration struct {
	Queue       string
	MaxMessages int
}

type IMessage interface {
	Body() []byte
	Ack() error
	GroupID() string
}

type Message struct {
	body                     []byte
	ack                      func() error
	groupId                  string
	visibilityFunctionCancel context.CancelFunc
}

func NewMessage(
	body []byte,
	ack func() error,
	groupId string,
	changeVisibility func(context.Context, int32) error,
	errorSub chan error,
) *Message {
	vc, cancel := context.WithCancel(context.Background())
	ticker := time.NewTicker(15 * time.Second)

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-vc.Done():
				return
			case <-ticker.C:
				if err := changeVisibility(vc, int32(30)); err != nil {
					if !errors.Is(err, context.Canceled) {
						errorSub <- err
					}
				}
			}
		}
	}()

	return &Message{body, ack, groupId, cancel}
}

func (m *Message) Body() []byte {
	return m.body
}

func (m *Message) Ack() error {
	m.visibilityFunctionCancel()
	return m.ack()
}

func (m *Message) GroupID() string {
	return m.groupId
}
