package events

import (
	"strconv"
	"time"

	"github.com/streadway/amqp"
)

// Event represents a single Bus event
type Event struct {
	Name string // Name of the event

	ID            string // Event ID
	CorrelationID string // ID of the correlated event
	AppID         string // Event sender application ID

	Priority   uint8         // Event priority 0..8
	Expiration time.Duration // Event expiration

	ContentType string // Event body content type
	Body        []byte // Event body

	tag uint64
	ack acknowledger
}

type acknowledger interface {
	Ack(tag uint64)
	Nack(tag uint64, requeue bool)
}

// Acknowledge successfully processing an incoming event
func (e *Event) Ack() {
	e.ack.Ack(e.tag)
}

// Negatively acknowledge failure to process an incoming event
// and optionally requeue the event
func (e *Event) Nack(requeue bool) {
	e.ack.Nack(e.tag, requeue)
}

// PublishFunc is an event publishing function to use from event handlers
type PublishFunc func(*Event) error

// EventRouter is a function that router event to handlers based on event name
type EventRouter func(*Event)

// EventHandler is a function that handles an event
//
// Handler can use PublishFunc to publish subsequent events that occur as a result of handling
// this event
type EventHandler func(*Event, PublishFunc)

// EventHandlers maps event name to event handler
type EventHandlers map[string]EventHandler

func eventFromDelivery(d *amqp.Delivery, ack acknowledger) *Event {
	return &Event{
		Name:          d.RoutingKey,
		ID:            d.MessageId,
		CorrelationID: d.CorrelationId,
		AppID:         d.AppId,
		Priority:      d.Priority,
		Expiration:    millisecondsStringToDuration(d.Expiration),
		ContentType:   d.ContentType,
		Body:          d.Body,

		tag: d.DeliveryTag,
		ack: ack,
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
	return strconv.FormatInt(d.Milliseconds(), 10)
}