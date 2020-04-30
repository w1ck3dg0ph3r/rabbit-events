package events

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"github.com/w1ck3dg0ph3r/rabbit-events/pkg/channel"
)

// Bus is a RabbitMQ based event bus client
//
// Queue for the app, as well as ingress and egress exchanges will be created if absent.
// Before starting the Bus, Connection, AppName and IngressExchange must be set.
type Bus struct {
	// RabbitMQ connection
	Connection ConnectionOpenCloser

	// AppName is a client application name
	// This name is used as event queue name and the AppID in event messages
	AppName string

	// Name of the exchange to receive events from
	IngressExchange string

	// Name of the exchange to send events to
	// Default is app name
	EgressExchange string

	// Time after which events will be declared dead and will be sent to DeadEventExchange
	EventTTL time.Duration

	// Exchange to which all dead events will be sent to
	DeadEventExchange string

	// Number of concurrent consumers to run
	// Default is the number of available processors
	ConcurrentConsumers int

	// Number of concurrent publishers to run
	// Default is 1
	ConcurrentPublishers int

	// How many messages could be in flight (unacked by handler) for consuming per consumer
	// Default is 100
	MaxEventsInFlight int

	// How many acks should be batched for sending to the broker
	// Default is same as MaxEventsInFlight
	AcksBatchSize int

	// Maximum amount of time between event being acked by handler and ack being sent to the broker
	// Default is 1s
	AcksMaxDelay time.Duration

	// How many messages could be in flight for publishing per publisher
	// Default is 100
	MaxPublishingsInFlight int

	// Time to wait for outstanding publishing confirms after Shutdown is called
	// Default is 5s
	ShutdownTimeout time.Duration

	Logger Logger

	started    int32
	done       chan error
	quit       chan struct{}
	shouldQuit chan error

	handlersM sync.RWMutex
	handlers  map[string]EventHandler

	topologyM     sync.Mutex
	topologySetUp bool
	topologyChan  channel.Channel

	consumers   []*consumer
	publishers  []*publisher
	publishings chan publishing
}

type Logger interface {
	Debugf(string, ...interface{})
	Infof(string, ...interface{})
}

type ConnectionOpenCloser interface {
	Open() (channel.Channel, error)
	Close() error
}

const (
	defaultConcurrentPublishers   = 1
	defaultMaxEventsInFlight      = 100
	defaultAcksMaxDelay           = 1 * time.Second
	defaultMaxPublishingsInFlight = 100
	defaultShutdownTimeout        = 5 * time.Second
)

// Start starts event bus
//
// Start returns immediately after starting an event bus. Use Shutdown to stop the started event bus
func (bus *Bus) Start() (err error) {
	err = bus.init()
	if err != nil {
		return
	}

	wg := &sync.WaitGroup{}
	bus.startPublishers(wg)
	bus.startConsumers(wg)
	atomic.StoreInt32(&bus.started, 1)

	go func() {
		<-bus.shouldQuit
		bus.Shutdown()
	}()

	go func() {
		wg.Wait()
		atomic.StoreInt32(&bus.started, 0)
		close(bus.done)
	}()

	return
}

// Run starts event bus and blocks until it is finished
func (bus *Bus) Run() (err error) {
	err = bus.init()
	if err != nil {
		return
	}

	wg := &sync.WaitGroup{}
	bus.startPublishers(wg)
	bus.startConsumers(wg)
	atomic.StoreInt32(&bus.started, 1)

	go func() {
		err = <-bus.shouldQuit
		bus.Shutdown()
	}()

	wg.Wait()
	atomic.StoreInt32(&bus.started, 0)
	close(bus.done)

	return
}

// SetHandler adds handler for event
//
// This opens connection to the broker and creates a binding between the exchange and the app queue
func (bus *Bus) SetHandler(event string, handler EventHandler) (err error) {
	err = bus.ensureTopology()
	if err != nil {
		return
	}
	bus.handlersM.Lock()
	defer bus.handlersM.Unlock()

	if bus.handlers == nil {
		bus.handlers = make(map[string]EventHandler)
	}
	bus.handlers[event] = handler
	err = bus.topologyChan.QueueBind(bus.AppName, event, bus.IngressExchange, false, nil)
	return
}

// Publish publishes an event and blocks until publishing is confirmed or rejected by the broker
func (bus *Bus) Publish(e *Event) (err error) {
	p := newPublishing(e)
	bus.publishings <- p
	<-p.done
	err = p.err
	return
}

// Shutdown stops event bus, waiting for all outstanding messages and then
// closes the connection to the broker
func (bus *Bus) Shutdown() {
	if atomic.LoadInt32(&bus.started) == 0 {
		return
	}
	close(bus.quit)
	<-bus.done
	if err := bus.topologyChan.Close(); err != nil {
		bus.debugf("error closing topology channel: %v", err)
	}
	if err := bus.Connection.Close(); err != nil {
		bus.debugf("error closing connection: %v", err)
	}
	atomic.StoreInt32(&bus.started, 0)
}

func (bus *Bus) init() (err error) {
	if atomic.LoadInt32(&bus.started) > 0 {
		panic("trying to start already started event bus")
	}

	bus.setDefaults()

	bus.quit = make(chan struct{})
	bus.shouldQuit = make(chan error)
	bus.done = make(chan error)

	err = bus.ensureTopology()
	if err != nil {
		err = errors.Wrap(err, "cant setup topology")
		bus.debugf(err.Error())
		return
	}
	return
}

func (bus *Bus) startConsumers(wg *sync.WaitGroup) {
	bus.consumers = make([]*consumer, bus.ConcurrentConsumers)
	wg.Add(bus.ConcurrentConsumers)
	for id := 0; id < bus.ConcurrentConsumers; id++ {
		bus.consumers[id] = &consumer{
			ID:                id,
			Connection:        bus.Connection,
			QueueName:         bus.AppName,
			MaxEventsInFlight: bus.MaxEventsInFlight,
			AcksBatchSize:     bus.AcksBatchSize,
			AcksMaxDelay:      bus.AcksMaxDelay,
			EventHandler:      bus.handleEvent,
			ShouldQuit:        bus.shouldQuit,
			Logger:            bus.Logger,
		}
		go bus.consumers[id].Run(wg, bus.quit)
	}
}

func (bus *Bus) startPublishers(wg *sync.WaitGroup) {
	bus.publishers = make([]*publisher, bus.ConcurrentPublishers)
	bus.publishings = make(chan publishing, bus.MaxPublishingsInFlight*bus.ConcurrentPublishers)
	wg.Add(bus.ConcurrentPublishers)
	for id := 0; id < bus.ConcurrentPublishers; id++ {
		bus.publishers[id] = &publisher{
			ID:                     id,
			Connection:             bus.Connection,
			Exchange:               bus.EgressExchange,
			ShutdownTimeout:        bus.ShutdownTimeout,
			MaxPublishingsInFlight: bus.MaxPublishingsInFlight,
			Publishings:            bus.publishings,
			ShouldQuit:             bus.shouldQuit,
			Logger:                 bus.Logger,
		}
		go bus.publishers[id].Run(wg, bus.quit)
	}
}

// handleEvent routes given event msg to appropriate handler
func (bus *Bus) handleEvent(e *Event) {
	bus.handlersM.RLock()
	defer bus.handlersM.RUnlock()

	handler, ok := bus.handlers[e.Name]
	if !ok {
		bus.debugf("unknown event from %s(%s): %s", e.AppID, e.ID, e.Name)
		return
	}
	go handler(e, bus.Publish)
}

// ensureTopology creates associated exchanges, queues and bindings
func (bus *Bus) ensureTopology() (err error) {
	bus.topologyM.Lock()
	defer bus.topologyM.Unlock()
	if bus.topologySetUp {
		return
	}
	bus.topologyChan, err = bus.Connection.Open()
	if err != nil {
		return
	}
	err = bus.topologyChan.ExchangeDeclare(bus.IngressExchange, "topic", true, false, false, false, nil)
	if err != nil {
		return
	}
	err = bus.topologyChan.ExchangeDeclare(bus.EgressExchange, "topic", true, false, false, false, nil)
	if err != nil {
		return
	}
	var args amqp.Table
	if bus.EventTTL > 0 && bus.DeadEventExchange != "" {
		args = amqp.Table{
			"x-message-ttl":          int64(bus.EventTTL / time.Millisecond),
			"x-dead-letter-exchange": bus.DeadEventExchange,
		}
	}
	_, err = bus.topologyChan.QueueDeclare(bus.AppName, true, false, false, false, args)
	if err != nil {
		return
	}
	bus.topologySetUp = true
	return
}

func (bus *Bus) setDefaults() {
	if bus.EgressExchange == "" {
		bus.EgressExchange = bus.AppName
	}
	if bus.ConcurrentConsumers == 0 {
		bus.ConcurrentConsumers = runtime.NumCPU()
	}
	if bus.ConcurrentPublishers == 0 {
		bus.ConcurrentPublishers = defaultConcurrentPublishers
	}
	if bus.MaxEventsInFlight == 0 {
		bus.MaxEventsInFlight = defaultMaxEventsInFlight
	}
	if bus.AcksBatchSize == 0 {
		bus.AcksBatchSize = bus.MaxEventsInFlight
	}
	if bus.AcksMaxDelay == 0 {
		bus.AcksMaxDelay = defaultAcksMaxDelay
	}
	if bus.MaxPublishingsInFlight == 0 {
		bus.MaxPublishingsInFlight = defaultMaxPublishingsInFlight
	}
	if bus.ShutdownTimeout == 0 {
		bus.ShutdownTimeout = defaultShutdownTimeout
	}
}

func (bus *Bus) debugf(f string, a ...interface{}) {
	if bus.Logger != nil {
		bus.Logger.Debugf(f, a...)
	}
}
