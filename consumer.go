package events

import (
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"

	"sync/atomic"

	"github.com/w1ck3dg0ph3r/rabbit-events/pkg/channel"
	"github.com/w1ck3dg0ph3r/rabbit-events/pkg/resequencer"
)

// consumer is an event consuming worker
//
// Encapsulates AMQP channel that is configured for consuming messages. When message is
// received on the channel, consumer will call back the Bus to handle that message.
type consumer struct {
	ID int

	Connection        ConnectionOpenCloser
	QueueName         string
	MaxEventsInFlight int
	AcksBatchSize     int
	AcksMaxDelay      time.Duration

	EventRouter EventRouter

	ShouldQuit chan error

	Logger Logger

	ch channel.Channel

	err  chan *amqp.Error
	msgs <-chan amqp.Delivery

	acknowledgements *resequencer.Resequencer
	lastacked        uint64
	lastsequenced    uint64
}

// Run loops over received messages until quit is closed or unrecoverable
// protocol error occurs
func (c *consumer) Run(wg *sync.WaitGroup, quit <-chan struct{}) {
	defer wg.Done()
	c.debugf("running event consumer %d", c.ID)
	err := c.createChannel()
	if err != nil {
		err = errors.Wrap(err, "cant create consumer channel")
		c.ShouldQuit <- err
		return
	}
	ackTicker := time.NewTicker(c.AcksMaxDelay)
	for {
		select {
		case msg, ok := <-c.msgs:
			if ok {
				e := eventFromDelivery(&msg, c)
				c.EventRouter(e)
			}
		case tag := <-c.acknowledgements.Out:
			atomic.StoreUint64(&c.lastsequenced, tag)
			if tag-atomic.LoadUint64(&c.lastacked) >= uint64(c.AcksBatchSize) {
				c.ackSequenced()
				ackTicker = time.NewTicker(c.AcksMaxDelay)
			}
		case <-ackTicker.C:
			c.ackSequenced()
			c.ackUnsequenced()
		case amqpErr, ok := <-c.err:
			if ok {
				if err := c.handleProtocolError(amqpErr); err != nil {
					return
				}
			}
		case <-quit:
			c.debugf("stopping event consumer %d", c.ID)
			c.ackSequenced()
			c.ackUnsequenced()
			return
		}
	}
}

func (c *consumer) Ack(tag uint64) {
	if tag < atomic.LoadUint64(&c.lastacked) {
		ignoreError(c.ch.Ack(tag, false))
		return
	}
	c.acknowledgements.Sequence(tag)
}

func (c *consumer) Nack(tag uint64, requeue bool) {
	if tag < atomic.LoadUint64(&c.lastsequenced) {
		c.ackSequenced()
		c.acknowledgements.Reset(tag, func(n uint64) {
			ignoreError(c.ch.Ack(n, false))
		})
		atomic.StoreUint64(&c.lastacked, tag)
	} else {
		c.ackSequenced()
		c.ackUnsequenced()
		c.acknowledgements.StartAt(tag)
		atomic.StoreUint64(&c.lastacked, tag)
	}
	ignoreError(c.ch.Nack(tag, false, requeue))
}

func (c *consumer) ackSequenced() {
	lastsequenced := atomic.LoadUint64(&c.lastsequenced)
	lastacked := atomic.LoadUint64(&c.lastacked)
	if lastsequenced > lastacked {
		ignoreError(c.ch.Ack(lastsequenced, true))
		atomic.StoreUint64(&c.lastacked, lastsequenced)
	}
}

func (c *consumer) ackUnsequenced() {
	c.acknowledgements.DumpUnsequenced(func(n uint64) {
		ignoreError(c.ch.Ack(n, false))
		if n > atomic.LoadUint64(&c.lastacked) {
			atomic.StoreUint64(&c.lastacked, n)
		}
	})
}

// createChannel opens and configures for consuming AMQP channel
func (c *consumer) createChannel() (err error) {
	c.ch, err = c.Connection.Open()
	if err != nil {
		return
	}

	err = c.ch.Qos(c.MaxEventsInFlight, 0, false)
	if err != nil {
		return
	}

	c.err = make(chan *amqp.Error, 1)
	c.err = c.ch.NotifyClose(c.err)

	c.acknowledgements = resequencer.New(c.MaxEventsInFlight)

	consumer := fmt.Sprintf("%s_%d", c.QueueName, c.ID)
	c.msgs, err = c.ch.Consume(c.QueueName, consumer, false, false, false, false, nil)
	return
}

func (c *consumer) handleProtocolError(amqpErr error) (err error) {
	c.debugf("consumer %d error: %s", c.ID, amqpErr)
	err = c.createChannel()
	if err != nil {
		c.debugf("cant recreate consumer channel %d: %s", c.ID, err)
		c.ShouldQuit <- err
		return
	}
	return
}

func ignoreError(err error) {
	_ = err
}

func (c *consumer) debugf(f string, a ...interface{}) {
	if c.Logger != nil {
		c.Logger.Debugf(f, a...)
	}
}
