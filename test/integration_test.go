//+build integration

package test_test

import (
	"bytes"
	"fmt"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	events "github.com/w1ck3dg0ph3r/rabbit-events"
)

type Logger struct{}

func (Logger) Infof(s string, a ...interface{})  { fmt.Printf(s+"\n", a...) }
func (Logger) Debugf(s string, a ...interface{}) { fmt.Printf(s+"\n", a...) }

func eventBus() *events.Bus {
	return &events.Bus{
		Connection: &events.Connection{
			Protocol:          "amqp",
			Hostnames:         []string{"127.0.0.1:5672"},
			Vhost:             "",
			Username:          "user",
			Password:          "pass",
			DialTimeout:       2 * time.Second,
			ConnectionTimeout: 30 * time.Second,
			ConnectionBackoff: 2 * time.Second,
			Logger:            nil,
		},
		AppName:              "app1",
		IngressExchange:      "i1",
		EgressExchange:       "i1",
		EventTTL:             60 * time.Second,
		DeadEventExchange:    "amq.fanout",
		ConcurrentPublishers: 2,
		ShutdownTimeout:      1 * time.Second,
		//Logger:               Logger{},
	}
}

func TestPublishConsume(t *testing.T) {
	t.Parallel()
	bus := eventBus()

	var payload = []byte("TEST")
	const count = 100
	var recv int32

	if err := bus.AddHandler("e1", func(e *events.Event, pub events.PublishFunc) {
		if !bytes.Equal(payload, e.Body) {
			t.Fail()
		}
		atomic.AddInt32(&recv, 1)
		e.Ack()
	}); err != nil {
		t.Fatal(err)
	}
	if err := bus.AddHandler("e2", func(e *events.Event, pub events.PublishFunc) {
		if !bytes.Equal(payload, e.Body) {
			t.Fail()
		}
		atomic.AddInt32(&recv, 1)
		e.Ack()
	}); err != nil {
		t.Fatal(err)
	}
	if err := bus.Start(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < int(count); i++ {
		var event string
		if i%2 == 0 {
			event = "e1"
		} else {
			event = "e2"
		}
		_ = bus.Publish(&events.Event{
			Name: event,
			ID:   strconv.Itoa(i + 1),
			Body: payload,
		})
	}
	bus.Shutdown()
	if count != recv {
		fmt.Println(count)
		t.Fail()
	}
}

func BenchmarkPublish(b *testing.B) {
	bus := eventBus()
	if err := bus.Start(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	var id uint64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mid := atomic.AddUint64(&id, 1)
			err := bus.Publish(&events.Event{
				Name:        "event.happened",
				ID:          strconv.Itoa(int(mid)),
				ContentType: "application/json",
				Body:        eventBody,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	bus.Shutdown()
}

func BenchmarkConsume(b *testing.B) {
	bus := eventBus()
	counter := int64(b.N)
	if err := bus.AddHandler("event.test", func(e *events.Event, pub events.PublishFunc) {
		atomic.AddInt64(&counter, -1)
		e.Ack()
	}); err != nil {
		b.Fatal(err)
	}
	if err := bus.Start(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := bus.Publish(&events.Event{
				Name:        "event.test",
				ContentType: "application/json",
				Body:        eventBody,
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	for {
		if c := atomic.LoadInt64(&counter); c == 0 {
			break
		}
		runtime.Gosched()
	}
	bus.Shutdown()
}

var eventBody = []byte(`{
	"guid": "c93a6a7a-3018-4476-8c95-a60a31c45291",
	"isActive": false,
	"balance": "$1,569.58",
	"age": 36,
	"eyeColor": "green",
	"name": "Alicia Valentine",
	"gender": "female",
	"company": "OMATOM",
	"email": "aliciavalentine@omatom.com",
	"phone": "+1 (966) 494-3993"
}`)
