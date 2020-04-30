package events

import (
	"math/rand"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"

	"github.com/w1ck3dg0ph3r/rabbit-events/pkg/channel"
)

// Connection is a message broker connection
//
// Connection handles reconnects and multiple clustered nodes automatically
type Connection struct {
	// Protocol is "amqp" or "amqps", default is amqps
	Protocol string

	// Hostnames is a slice of AMQP broker hostname[:port] pairs to connect to
	//
	// Connection connect to one of those randomly
	Hostnames []string

	Vhost    string
	Username string
	Password string

	// Connection timeout to a single broker
	DialTimeout time.Duration

	// Overall connection timeout
	ConnectionTimeout time.Duration

	// Backoff between connection attempts
	ConnectionBackoff time.Duration

	Logger Logger

	connM sync.Mutex
	conn  *amqp.Connection

	currentURL string

	defaultsSet bool
}

var (
	errNoBrokers         = errors.New("no brokers specified")
	errConnectionTimeout = errors.New("connection timeout")
)

// Channel returns AMQP channel, connecting or reconnecting to the broker if necessary
//
// Clients need to reacquire channel on any errors and when channel or underlying
// connection closes. See github.com/streadway/amqp documentation for details.
func (c *Connection) Open() (channel channel.Channel, err error) {
	c.connM.Lock()
	defer c.connM.Unlock()

	if c.conn == nil || c.conn.IsClosed() {
		err = c.connect()
		if err != nil {
			return
		}
	}
	channel, err = c.conn.Channel()

	return
}

// Close closes the connection
func (c *Connection) Close() (err error) {
	c.connM.Lock()
	defer c.connM.Unlock()

	if c.conn != nil {
		err = c.conn.Close()
	}

	return
}

func (c *Connection) connect() (err error) {
	if len(c.Hostnames) == 0 {
		return errNoBrokers
	}

	if !c.defaultsSet {
		c.setDefaults()
		c.defaultsSet = true
	}

	timeout := time.After(c.ConnectionTimeout)
	done := make(chan struct{})
	go c.connectRandomNode(done)

	select {
	case <-done:
		return nil
	case <-timeout:
		return errConnectionTimeout
	}
}

func (c *Connection) connectRandomNode(done chan<- struct{}) {
	delay := 1 * time.Second
	for {
		randomHostname := c.Hostnames[rand.Intn(len(c.Hostnames))]
		randomUrl := c.Protocol + "://" + c.Username + ":" + c.Password + "@" + randomHostname + "/" + c.Vhost
		err := c.connectNode(randomUrl)
		if err != nil {
			if c.Logger != nil {
				c.Logger.Debugf("error connecting to %s: %v", randomHostname, err)
			}
			time.Sleep(delay)
			delay += c.ConnectionBackoff
			continue
		}
		c.currentURL = randomUrl
		if c.Logger != nil {
			c.Logger.Infof("connected to %s", randomHostname)
		}
		close(done)
		return
	}
}

func (c *Connection) connectNode(url string) (err error) {
	dialConfig := amqp.Config{
		Dial: amqp.DefaultDial(c.DialTimeout),
	}
	c.conn, err = amqp.DialConfig(url, dialConfig)
	if err != nil {
		return
	}
	return
}

func (c *Connection) setDefaults() {
	if c.DialTimeout == 0 {
		c.DialTimeout = 3 * time.Second
	}
	if c.ConnectionTimeout == 0 {
		c.ConnectionTimeout = 10 * time.Second
	}
	if c.ConnectionBackoff == 0 {
		c.ConnectionBackoff = 3 * time.Second
	}
	if c.Protocol == "" {
		c.Protocol = "amqps"
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
