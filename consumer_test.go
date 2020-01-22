package events

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/mock"
	"github.com/w1ck3dg0ph3r/rabbit-events/test/mocks"
)

func TestConsumer_SetupsChannelInConsumeMode(t *testing.T) {
	ch := &mocks.Channel{}
	conn := &MockConn{ch}
	c := consumer{
		Connection:   conn,
		AcksMaxDelay: 1 * time.Second,
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	quit := make(chan struct{})

	ch.On("Qos", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	ch.On("NotifyClose", mock.Anything).Return(nil).Once()
	ch.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).Return(nil, nil).Once()

	go func() {
		c.Run(&wg, quit)
	}()

	quit <- struct{}{}
	wg.Wait()
	ch.AssertExpectations(t)
}

func TestConsumer_RecreatesChannelOnError(t *testing.T) {
	ch := &mocks.Channel{}
	conn := &MockConn{ch}
	c := consumer{
		Connection:   conn,
		AcksMaxDelay: 1 * time.Second,
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	quit := make(chan struct{})

	var errch chan *amqp.Error
	chopened := make(chan bool, 1)

	ch.On("Qos", mock.Anything, mock.Anything, mock.Anything).Return(nil).Twice()
	ch.On("NotifyClose", mock.Anything).Return(func(e chan *amqp.Error) chan *amqp.Error {
		errch = e
		chopened <- true
		return e
	}).Twice()
	ch.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).Return(nil, nil).Twice()

	go func() {
		c.Run(&wg, quit)
	}()

	// when channel is opened, imitate protocol error
	<-chopened
	errch <- &amqp.Error{}
	runtime.Gosched()
	_ = errch

	quit <- struct{}{}
	wg.Wait()
	ch.AssertExpectations(t)
}

func TestConsumer_BatchesAcks(t *testing.T) {
	ch := &mocks.Channel{}
	conn := &MockConn{ch}
	c := consumer{
		Connection:        conn,
		MaxEventsInFlight: 5,
		AcksBatchSize:     3,
		AcksMaxDelay:      100 * time.Minute,
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	quit := make(chan struct{}, 1)

	deliveries := make(chan amqp.Delivery, 5)
	c.EventHandler = func(e *Event) {
		e.Ack()
	}

	ch.On("Qos", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	ch.On("NotifyClose", mock.Anything).Return(nil).Once()
	ch.On("Consume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
		mock.Anything, mock.Anything).Return((<-chan amqp.Delivery)(deliveries), nil).Once()

	ch.On("Ack", uint64(3), true).Return(nil).Once()
	ch.On("Ack", uint64(5), true).Return(nil).Once()

	go func() {
		c.Run(&wg, quit)
	}()

	for i := uint64(1); i <= 5; i++ {
		deliveries <- amqp.Delivery{DeliveryTag: i}
	}
	time.Sleep(100 * time.Millisecond)

	quit <- struct{}{}
	wg.Wait()
	ch.AssertExpectations(t)
}
