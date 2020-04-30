package events

import (
	"strconv"
	"time"

	"github.com/streadway/amqp"
)

// Event represents a single event bus event
type Event struct {
	Name string // Name of the event

	ID            string // Event ID
	CorrelationID string // ID of the correlated event
	AppID         string // Event sender application ID

	Priority   uint8         // Event priority 0..8
	Expiration time.Duration // Event expiration

	ContentType string // Event body content type
	Body        []byte // Event body

	tag   uint64
	acker acknowledger
}

type acknowledger interface {
	Ack(tag uint64)
	Nack(tag uint64, requeue bool)
}

// Ack acknowledges successfully processing an incoming event
func (e *Event) Ack() {
	e.acker.Ack(e.tag)
}

// Nack acknowledges failure to process an incoming event
// and optionally requests to requeue the event
func (e *Event) Nack(requeue bool) {
	e.acker.Nack(e.tag, requeue)
}

// PublishFunc is an event publishing function to use from event handlers
type PublishFunc func(*Event) error

// EventRouter is a function that routes event to handler based on event name
type EventRouter func(*Event)

// EventHandler is a function that handles an event
//
// Handler can use PublishFunc to publish subsequent events that occur as a result of handling
// this event
type EventHandler func(*Event, PublishFunc)

func eventFromDelivery(d *amqp.Delivery, acker acknowledger) *Event {
	return &Event{
		Name:          d.RoutingKey,
		ID:            d.MessageId,
		CorrelationID: d.CorrelationId,
		AppID:         d.AppId,
		Priority:      d.Priority,
		Expiration:    millisecondsStringToDuration(d.Expiration),
		ContentType:   d.ContentType,
		Body:          d.Body,

		tag:   d.DeliveryTag,
		acker: acker,
	}
}

func eventToPublishing(e *Event) *amqp.Publishing {
	return &amqp.Publishing{
		AppId:         e.AppID,
		MessageId:     e.ID,
		CorrelationId: e.CorrelationID,
		DeliveryMode:  amqp.Persistent,
		Priority:      e.Priority,
		Expiration:    durationToMillisecondsString(e.Expiration),
		ContentType:   e.ContentType,
		Body:          e.Body,
	}
}

func millisecondsStringToDuration(s string) time.Duration {
	if s == "" {
		return 0
	}
	res, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return time.Duration(res) * time.Millisecond
}

func durationToMillisecondsString(d time.Duration) string {
	if d == 0 {
		return ""
	}
	return strconv.FormatInt(int64(d/time.Millisecond), 10)
}
