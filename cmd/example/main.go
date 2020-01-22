package main

import (
	"fmt"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	events "github.com/w1ck3dg0ph3r/rabbit-events"
)

var log = &logrus.Logger{
	Out:       os.Stderr,
	Level:     logrus.DebugLevel,
	Formatter: &logrus.TextFormatter{},
}

func main() {
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
			Logger:            log,
		},
		AppName:              "testapp",
		IngressExchange:      "ingress",
		EgressExchange:       "ingress",
		EventTTL:             60 * time.Second,
		DeadEventExchange:    "amq.fanout",
		ConcurrentConsumers:  4,
		ConcurrentPublishers: 2,
		MaxEventsInFlight:    100,
		Logger:               log,

		EventHandlers: events.EventHandlers{
			"event1": func(e *events.Event, publish events.PublishFunc) {
				fmt.Printf("event1 happened (id:%s, cid:%s)\n", e.ID, e.CorrelationID)
				err := publish(&events.Event{Name: "event2", ID: "id2",
					CorrelationID: e.ID})
				if err != nil {
					fmt.Println(err)
					e.Nack(false)
					return
				}
				e.Ack()
			},
			"event2": func(e *events.Event, publish events.PublishFunc) {
				fmt.Printf("event2 happened (id:%s, cid:%s)\n", e.ID, e.CorrelationID)
				e.Ack()
			},
		},
	}

	// Run event bus
	if err := bus.Start(); err != nil {
		log.Fatal(err)
	}

	// Publish an event
	err := bus.Publish(&events.Event{Name: "event1", ID: "id1"})
	if err != nil {
		panic(err)
	}
	time.Sleep(1 * time.Second)

	// Stop event bus
	bus.Shutdown()
}
