package events_test

import (
	"fmt"
	"log"
	"time"

	events "github.com/w1ck3dg0ph3r/rabbit-events"
)

// This example shows basics of using event bus
func Example() {
	// Create an event bus
	bus := &events.Bus{
		Connection: &events.Connection{
			Protocol:          "amqp",
			Hostnames:         []string{"127.0.0.1:5672"},
			Vhost:             "",
			Username:          "user",
			Password:          "pass",
			DialTimeout:       1 * time.Second,
			ConnectionTimeout: 10 * time.Second,
			ConnectionBackoff: 1 * time.Second,
			Logger:            nil,
		},
		AppName:              "testapp",
		IngressExchange:      "ingress",
		EgressExchange:       "ingress",
		EventTTL:             60 * time.Second,
		DeadEventExchange:    "amq.fanout",
		ConcurrentConsumers:  4,
		ConcurrentPublishers: 4,
		MaxEventsInFlight:    100,
	}

	if err := bus.AddHandler("event.example", func(e *events.Event, publish events.PublishFunc) {
		fmt.Printf("id:%s, cid:%s\n", e.ID, e.CorrelationID)
		err := publish(&events.Event{Name: "event.second", ID: "id2", CorrelationID: e.ID})
		if err != nil {
			fmt.Println(err)
			e.Nack(false)
			return
		}
		e.Ack()
	}); err != nil {
		log.Fatal(err)
	}
	if err := bus.AddHandler("event.second", func(e *events.Event, publish events.PublishFunc) {
		fmt.Printf("id:%s, cid:%s\n", e.ID, e.CorrelationID)
		e.Ack()
	}); err != nil {
		log.Fatal(err)
	}

	// Run event bus
	if err := bus.Start(); err != nil {
		log.Fatal(err)
	}

	// Publish an event
	err := bus.Publish(&events.Event{Name: "event.example", ID: "id1"})
	if err != nil {
		panic(err)
	}
	time.Sleep(1 * time.Second)

	// Stop event bus
	bus.Shutdown()

	// Output:
	// id:id1, cid:
	// id:id2, cid:id1
}
