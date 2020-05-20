package natsutil

import (
	"context"
	"net/http"

	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

// MsgHandler represent an handler for a NATS subscriber
type MsgHandler func(nc *nats.Conn, msg *nats.Msg) error

// Worker is used for asynchronous job pooling
type Worker func(id int, jobs <-chan *nats.Msg, running chan<- int, httpClient *http.Client, nc *nats.Conn)

// Subscriber represent a NATS subscriber
type Subscriber struct{ nc *nats.Conn }

// NewSubscriber create a new subscriber and connect it to given NATS server
func NewSubscriber(address string) (*Subscriber, error) {
	nc, err := nats.Connect(address)
	if err != nil {
		logrus.Errorf("Error while connecting to NATS server %s: %s", address, err)
		return nil, err
	}

	return &Subscriber{
		nc: nc,
	}, nil
}

// QueueSubscribe subscribe to given subject, with given queue
func (qs *Subscriber) QueueSubscribe(subject, queue string, handler MsgHandler) error {
	// Create the subscriber
	sub, err := qs.nc.QueueSubscribeSync(subject, queue)
	if err != nil {
		logrus.Errorf("Error while reading message from NATS server: %s", err)
		return err
	}

	for {
		// Read incoming message
		msg, err := sub.NextMsgWithContext(context.Background())
		if err != nil {
			logrus.Warnf("Skipping current message because of error: %s", err)
			continue
		}

		// ... And process it
		if err := handler(qs.nc, msg); err != nil {
			logrus.Warnf("Skipping current message because of error: %s", err)
			continue
		}
	}
}

// QueueSubscribeAsync subscribe to given subject, with given queue and work is handled asynchronously.
// Will sit in this function forever
func (qs *Subscriber) QueueSubscribeAsync(subject, queue string, worker Worker, workerCount int, httpClient *http.Client) error {
	jobs := make(chan *nats.Msg, 1500)
	results := make(chan int, 100)

	for i := 0; i < workerCount; i++ {
		go worker(i, jobs, results, httpClient, qs.nc)
		logrus.Info("Starting worker id:", i)
	}

	// Create the subscriber
	sub, err := qs.nc.QueueSubscribeSync(subject, queue)
	if err != nil {
		logrus.Errorf("Error while reading message from NATS server: %s", err)
		return err
	}

	for {
		// Read incoming message
		msg, err := sub.NextMsgWithContext(context.Background())
		if err != nil {
			logrus.Warnf("Skipping current message because of error: %s", err)
			continue
		}

		jobs <- msg
		logrus.Info("Total queued crawls:", len(jobs))
	}
}

// Close terminate the connection to the NATS server
func (qs *Subscriber) Close() {
	qs.nc.Close()
}
