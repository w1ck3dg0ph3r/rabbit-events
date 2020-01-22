package channel

import "github.com/streadway/amqp"

//go:generate mockery -name Channel -case snake -output ../../test/mocks

type Channel interface {
	Close() error
	NotifyClose(c chan *amqp.Error) chan *amqp.Error
	NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation
	Qos(prefetchCount, prefetchSize int, global bool) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	ExchangeBind(destination, key, source string, noWait bool, args amqp.Table) error
	ExchangeUnbind(destination, key, source string, noWait bool, args amqp.Table) error
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Confirm(noWait bool) error
	Ack(tag uint64, multiple bool) error
	Nack(tag uint64, multiple bool, requeue bool) error
	Reject(tag uint64, requeue bool) error
}
