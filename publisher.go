package events

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/w1ck3dg0ph3r/rabbit-events/pkg/channel"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

// publisher is an event publisher of the Bus
type publisher struct {
	ID int

	Connection             ConnectionOpenCloser
	Exchange               string
	ShutdownTimeout        time.Duration
	MaxPublishingsInFlight int

	Publishings chan publishing

	ShouldQuit chan error

	Logger Logger

	ch channel.Channel

	err      chan *amqp.Error
	confirms chan amqp.Confirmation

	// publishing delivery tag, starting with 1
	tag uint64

	// publishings in flight by delivery tag
	publishings map[uint64]publishing
}

type publishing struct {
	e    *Event
	done chan bool
	err  error
}

func newPublishing(e *Event) publishing {
	return publishing{e, make(chan bool), nil}
}

// Run loops over received publishing confirmations until quit is closed or unrecoverable
// connection error occurs
func (p *publisher) Run(wg *sync.WaitGroup, quit <-chan struct{}) {
	defer wg.Done()
	p.debugf("running event publisher %d", p.ID)
	err := p.createChannel()
	if err != nil {
		err = errors.Wrap(err, "cant create publisher channel")
		p.ShouldQuit <- err
		return
	}
	for {
		select {
		case pub := <-p.Publishings:
			msg := eventToPublishing(pub.e)
			err := p.ch.Publish(p.Exchange, pub.e.Name, false, false, *msg)
			if err != nil {
				break
			}
			p.tag++
			p.publishings[p.tag] = pub
		case c := <-p.confirms:
			pub := p.publishings[c.DeliveryTag]
			delete(p.publishings, c.DeliveryTag)
			if !c.Ack {
				pub.err = fmt.Errorf("publisher %d delivery %d nacked by broker", p.ID, c.DeliveryTag)
			}
			pub.done <- c.Ack
		case amqpErr, ok := <-p.err:
			if ok {
				if err := p.handleProtocolError(amqpErr); err != nil {
					return
				}
			}
		case <-quit:
			p.debugf("stopping event publisher %d", p.ID)
			waitForOutstandingPublishings(p.publishings, p.ShutdownTimeout)
			return
		}
	}
}

func (p *publisher) handleProtocolError(amqpErr error) (err error) {
	p.debugf("publisher %d error: %s", p.ID, amqpErr)
	p.nackOutstandingPublishings(amqpErr)
	err = p.createChannel()
	if err != nil {
		p.debugf("cant recreate publisher channel %d: %s", p.ID, err)
		p.ShouldQuit <- err
		return
	}
	return
}

// createChannel opens and configures for publishing AMQP channel
func (p *publisher) createChannel() (err error) {
	p.ch, err = p.Connection.Open()
	if err != nil {
		return
	}

	// put channel in confirm mode
	err = p.ch.Confirm(false)
	if err != nil {
		return
	}

	// subscribe to error notifications
	p.err = make(chan *amqp.Error, 1)
	p.ch.NotifyClose(p.err)

	// subscribe to publishing confirmations
	p.confirms = make(chan amqp.Confirmation, p.MaxPublishingsInFlight)
	p.ch.NotifyPublish(p.confirms)

	p.publishings = make(map[uint64]publishing, p.MaxPublishingsInFlight)
	p.tag = 0
	return
}

func (p *publisher) nackOutstandingPublishings(err error) {
	for tag := range p.publishings {
		pub := p.publishings[tag]
		pub.err = err
		pub.done <- false
	}
}

func waitForOutstandingPublishings(pubs map[uint64]publishing, timeout time.Duration) {
	done := make(chan struct{}, 1)
	go func() {
		for {
			l := len(pubs)
			if l == 0 {
				done <- struct{}{}
				return
			}
			runtime.Gosched()
		}
	}()
	select {
	case <-done:
	case <-time.After(timeout):
	}
}

func (p *publisher) debugf(f string, a ...interface{}) {
	if p.Logger != nil {
		p.Logger.Debugf(f, a...)
	}
}
