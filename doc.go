/*
Package events provides event bus implemented on top of RabbitMQ message broker.

Guaranties at least once semantics on event delivery. Notice that
event handling should be idempotent, otherwise additional event
deduplication is required to ensure that the same event is not handled
twice.

Features

- Connection to a RabbitMQ cluster with automatic reconnection

- Configurable concurrency for consuming and publishing

- Broker topology setup

- Batching of acknowledgements sent to broker

- Pluggable logging

Example

This example shows basics of using event bus

    package main

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

        if err := bus.SetHandler("event.example", func(e *events.Event, publish events.PublishFunc) {
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

        if err := bus.SetHandler("event.second", func(e *events.Event, publish events.PublishFunc) {
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
    }


See tests and runnable examples for additional info.

*/
package events
