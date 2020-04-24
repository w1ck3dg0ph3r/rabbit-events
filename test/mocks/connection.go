package mocks

import (
	"sync/atomic"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/mock"
	"github.com/w1ck3dg0ph3r/rabbit-events/pkg/channel"
)

type Chan struct {
	Channel

	Deliveries chan amqp.Delivery
	Confirms   chan amqp.Confirmation
	Err        chan *amqp.Error

	tag uint64
}

type Connection struct {
	Deliveries chan amqp.Delivery
	channels   []*Chan
}

func NewConnection() *Connection {
	c := &Connection{
		Deliveries: make(chan amqp.Delivery, 1),
	}
	return c
}

func (c *Connection) Open() (channel.Channel, error) {
	if c == nil {
		panic("open on nil connection")
	}

	ch := &Chan{
		Channel: Channel{},
	}

	ch.On("Close").Return(nil)

	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).Return(nil)

	ch.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).Return(amqp.Queue{}, nil)

	ch.On("QueueBind", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything).Return(nil)

	ch.On("Qos", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	ch.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).Return(func(queue, consumer string, autoAck, exclusive, noLocal,
		noWait bool, args amqp.Table) <-chan amqp.Delivery {
		ch.Deliveries = c.Deliveries
		return ch.Deliveries
	}, nil)

	ch.On("Confirm", mock.Anything).Return(nil)

	ch.On("NotifyClose", mock.Anything).Return(func(e chan *amqp.Error) chan *amqp.Error {
		ch.Err = e
		return e
	})

	ch.On("Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything).Return(func(exchange, key string, mandatory, immediate bool,
		msg amqp.Publishing) error {
		tag := atomic.AddUint64(&ch.tag, 1)
		go func(tag uint64) {
			c.Deliveries <- amqp.Delivery{
				Acknowledger:    ch,
				Headers:         msg.Headers,
				ContentType:     msg.ContentType,
				ContentEncoding: msg.ContentEncoding,
				DeliveryMode:    msg.DeliveryMode,
				Priority:        msg.Priority,
				CorrelationId:   msg.CorrelationId,
				ReplyTo:         msg.ReplyTo,
				Expiration:      msg.Expiration,
				MessageId:       msg.MessageId,
				Timestamp:       msg.Timestamp,
				Type:            msg.Type,
				UserId:          msg.UserId,
				AppId:           msg.AppId,
				DeliveryTag:     tag,
				Exchange:        exchange,
				RoutingKey:      key,
				Body:            msg.Body,
			}
			ch.Confirms <- amqp.Confirmation{
				DeliveryTag: tag,
				Ack:         true,
			}
		}(tag)
		return nil
	})

	ch.On("NotifyPublish", mock.Anything).Return(
		func(conf chan amqp.Confirmation) chan amqp.Confirmation {
			ch.Confirms = conf
			return conf
		})

	ch.On("Ack", mock.Anything, mock.Anything).Return(func(tag uint64, multiple bool) error {
		return nil
	})

	ch.On("Nack", mock.Anything, mock.Anything).Return(func(tag uint64, multiple bool,
		requeue bool) error {
		return nil
	})

	return ch, nil
}

func (c *Connection) Close() error {
	return nil
}
