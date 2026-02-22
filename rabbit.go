package broker

import (
	"context"
	"fmt"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
)

type RabbitBroker struct {
	conn   *amqp.Connection
	config *RabbitConfig
}

type RabbitConfig struct {
	URL string
}

func NewRabbit(cfg *RabbitConfig) IBroker {
	conn, err := amqp.Dial(cfg.URL)
	if err != nil {
		panic(fmt.Errorf("failed to connect to RabbitMQ: %w", err))
	}

	return &RabbitBroker{
		conn:   conn,
		config: cfg,
	}
}

func (r *RabbitBroker) Publish(ctx context.Context, queue string, groupId *string, data *string) error {
	ch, err := r.conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer ch.Close()

	// Assicuriamo che la coda esista
	_, err = ch.QueueDeclare(
		queue, // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	// Pubblica il messaggio
	err = ch.PublishWithContext(
		ctx,
		"",    // exchange
		queue, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:  "text/plain",
			Body:         []byte(*data),
			DeliveryMode: amqp.Persistent, // make message persistent
			MessageId:    *groupId,
			Headers: amqp.Table{
				"groupId": *groupId,
			},
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

func (r *RabbitBroker) Subscribe(ctx context.Context, conf Configuration, fn func(context.Context, IMessage)) error {
	ch, err := r.conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer ch.Close()

	// Assicuriamo che la coda esista
	_, err = ch.QueueDeclare(
		conf.Queue, // name
		true,       // durable
		false,      // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	// Impostiamo QoS per limitare il numero di messaggi non confermati
	err = ch.Qos(
		conf.MaxMessages, // prefetch count
		0,                // prefetch size
		false,            // global
	)
	if err != nil {
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	// Consumiamo i messaggi dalla coda
	msgs, err := ch.Consume(
		conf.Queue, // queue
		"",         // consumer
		false,      // auto-ack (false per gestire manualmente gli ack)
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %w", err)
	}

	var messageCount int32 = 0
	message := make(chan IMessage, conf.MaxMessages)
	errSub := make(chan error, conf.MaxMessages)

	g, gCtx := errgroup.WithContext(ctx)

	// Goroutine per ricevere i messaggi da RabbitMQ
	go func() {
		defer close(message)
		for {
			select {
			case <-gCtx.Done():
				return
			case d, ok := <-msgs:
				if !ok {
					errSub <- fmt.Errorf("channel closed")
					return
				}

				if atomic.LoadInt32(&messageCount) >= int32(conf.MaxMessages) {
					// Rifiuta il messaggio e lo rimette in coda
					d.Nack(false, true)
					continue
				}

				atomic.AddInt32(&messageCount, 1)

				groupId := ""
				if val, ok := d.Headers["groupId"].(string); ok {
					groupId = val
				} else if d.MessageId != "" {
					groupId = d.MessageId
				}

				message <- NewRabbitMessage(
					d.Body,
					func() error {
						return d.Ack(false)
					},
					groupId,
					errSub,
				)
			}
		}
	}()

	// Worker goroutines per processare i messaggi
	for i := 0; i < conf.MaxMessages; i++ {
		g.Go(func() error {
			for {
				select {
				case msg, ok := <-message:
					if !ok {
						return nil
					}
					fn(gCtx, msg)
					atomic.AddInt32(&messageCount, -1)
				case err := <-errSub:
					return err
				case <-gCtx.Done():
					return nil
				}
			}
		})
	}

	return g.Wait()
}

func NewRabbitMessage(
	body []byte,
	ack func() error,
	groupId string,
	errorSub chan error,
) *Message {
	vc, cancel := context.WithCancel(context.Background())

	go func() {
		<-vc.Done()
	}()

	return &Message{body, ack, groupId, cancel}
}
