package broker

import (
	"context"
	"strings"
	"time"
)

type IBroker interface {
	Publish(context.Context, string, string, *string) error
	Subscribe(context.Context, Configuration, func(context.Context, IMessage)) error
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

	go func() {
		for {
			select {
			case <-vc.Done():
				return
			case <-time.After(20 * time.Second):
				if err := changeVisibility(vc, int32(30)); err != nil {
					if !strings.HasSuffix(err.Error(), context.Canceled.Error()) {
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
