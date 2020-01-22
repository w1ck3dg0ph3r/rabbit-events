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

See tests and runnable examples for additional info.

*/
package events
