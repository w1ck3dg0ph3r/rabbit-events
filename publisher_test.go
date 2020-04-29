package events

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/w1ck3dg0ph3r/rabbit-events/pkg/channel"
	"github.com/w1ck3dg0ph3r/rabbit-events/test/mocks"
)

type MockConn struct {
	ch *mocks.Channel
}

func (c *MockConn) Open() (channel.Channel, error) {
	return c.ch, nil
}

func (c *MockConn) Close() error {
	return nil
}

func TestPublisher_SetupsChannelInConfirmMode(t *testing.T) {
	ch := &mocks.Channel{}
	conn := &MockConn{ch}
	p := publisher{
		Connection: conn,
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	quit := make(chan struct{})

	ch.On("Confirm", mock.Anything).Return(nil).Once()
	ch.On("NotifyClose", mock.Anything).Return(nil).Once()
	ch.On("NotifyPublish", mock.Anything).Return(nil).Once()

	go func() {
		p.Run(&wg, quit)
	}()

	quit <- struct{}{}
	wg.Wait()
	ch.AssertExpectations(t)
}

func TestPublisher_RecreatesChannelOnError(t *testing.T) {
	t.Parallel()

	ch := &mocks.Channel{}
	conn := &MockConn{ch}
	p := publisher{
		Connection: conn,
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	quit := make(chan struct{})

	var errch chan *amqp.Error
	chopened := make(chan bool, 1)

	ch.On("Confirm", mock.Anything).Return(nil).Twice()
	ch.On("NotifyClose", mock.Anything).Return(func(e chan *amqp.Error) chan *amqp.Error {
		errch = e
		chopened <- true
		return e
	}).Twice()
	ch.On("NotifyPublish", mock.Anything).Return(nil).Twice()

	go func() {
		p.Run(&wg, quit)
	}()

	// when channel is opened, imitate protocol error
	<-chopened
	errch <- &amqp.Error{}
	runtime.Gosched()
	time.Sleep(1 * time.Millisecond)

	quit <- struct{}{}
	wg.Wait()

	ch.AssertExpectations(t)
}

func TestPublisher_WaitsForConfirm(t *testing.T) {
	t.Parallel()

	ch := &mocks.Channel{}
	conn := &MockConn{ch}
	bus := Bus{
		Connection:             conn,
		ConcurrentConsumers:    1,
		ConcurrentPublishers:   1,
		MaxEventsInFlight:      1,
		MaxPublishingsInFlight: 1,
		ShutdownTimeout:        5 * time.Second,
	}

	ch.On("Close").Return(nil)
	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	ch.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).Return(amqp.Queue{}, nil)

	var confirmsM sync.RWMutex
	var confirms chan amqp.Confirmation

	ch.On("Qos", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	ch.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).Return(nil, nil)
	ch.On("Confirm", mock.Anything).Return(nil)
	ch.On("NotifyClose", mock.Anything).Return(nil)
	ch.On("Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	ch.On("NotifyPublish", mock.Anything).Return(
		func(c chan amqp.Confirmation) chan amqp.Confirmation {
			confirmsM.Lock()
			confirms = c
			confirmsM.Unlock()
			return c
		})

	if err := bus.Start(); err != nil {
		t.Fatal(err)
	}

	published := false
	publishedBeforeConfirm := false

	go func() {
		time.Sleep(100 * time.Millisecond)
		if published {
			publishedBeforeConfirm = true
		}
		confirmsM.RLock()
		confirms <- amqp.Confirmation{
			DeliveryTag: 1,
			Ack:         true,
		}
		confirmsM.RUnlock()
	}()

	if err := bus.Publish(&Event{}); err != nil {
		t.Fatal(err)
	}
	published = true

	bus.Shutdown()

	assert.False(t, publishedBeforeConfirm, "publish returned before confirmation")
}

func TestPublisher_WaitsForConfirmOnShutdown(t *testing.T) {
	t.Parallel()

	ch := &mocks.Channel{}
	conn := &MockConn{ch}
	bus := Bus{
		Connection:             conn,
		ConcurrentConsumers:    1,
		ConcurrentPublishers:   1,
		MaxEventsInFlight:      1,
		MaxPublishingsInFlight: 1,
		ShutdownTimeout:        5 * time.Second,
	}

	ch.On("Close").Return(nil)
	ch.On("ExchangeDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	ch.On("QueueDeclare", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).Return(amqp.Queue{}, nil)
	ch.On("QueueBind", mock.Anything).Return(nil)

	var confirmsM sync.RWMutex
	var confirms chan amqp.Confirmation

	ch.On("Qos", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	ch.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).Return(nil, nil)
	ch.On("Confirm", mock.Anything).Return(nil)
	ch.On("NotifyClose", mock.Anything).Return(nil)
	ch.On("Publish", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	ch.On("NotifyPublish", mock.Anything).Return(
		func(c chan amqp.Confirmation) chan amqp.Confirmation {
			confirmsM.Lock()
			confirms = c
			confirmsM.Unlock()
			return c
		})

	if err := bus.Start(); err != nil {
		t.Fatal(err)
	}

	shutdown := false
	shutdownBeforeConfirm := false

	go func() {
		time.Sleep(100 * time.Millisecond)
		if shutdown {
			shutdownBeforeConfirm = true
		}
		confirmsM.RLock()
		confirms <- amqp.Confirmation{
			DeliveryTag: 1,
			Ack:         true,
		}
		confirmsM.RUnlock()
	}()

	down := make(chan bool)

	go func() {
		_ = bus.Publish(&Event{})
		bus.Shutdown()
		shutdown = true
		down <- true
	}()

	<-down
	assert.False(t, shutdownBeforeConfirm, "shutdown before confirmation")
}
