package mqtt

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
)

type Client interface {
	StoreTopic(*Topic)
	GetTopic(string) (*Topic, bool)
	IsConnected() bool
	Subscribe(*Topic) error
	Unsubscribe(*Topic)
	Dispose()
}

type Options struct {
	URI      string `json:"uri"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type client struct {
	client paho.Client
	topics TopicMap
	mu     sync.Mutex
}

func NewClient(o Options) (Client, error) {
	opts := paho.NewClientOptions()

	opts.AddBroker(o.URI)
	opts.SetClientID(fmt.Sprintf("grafana_%d", rand.Int()))

	if o.Username != "" {
		opts.SetUsername(o.Username)
	}

	if o.Password != "" {
		opts.SetPassword(o.Password)
	}

	opts.SetPingTimeout(60 * time.Second)
	opts.SetKeepAlive(60 * time.Second)
	opts.SetAutoReconnect(true)
	opts.SetCleanSession(false)
	opts.SetMaxReconnectInterval(10 * time.Second)
	opts.SetConnectionLostHandler(func(c paho.Client, err error) {
		log.DefaultLogger.Error("MQTT Connection lost", "error", err)
	})
	opts.SetReconnectingHandler(func(c paho.Client, options *paho.ClientOptions) {
		log.DefaultLogger.Debug("MQTT Reconnecting")
	})

	log.DefaultLogger.Info("MQTT Connecting")

	pahoClient := paho.NewClient(opts)
	if token := pahoClient.Connect(); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("error connecting to MQTT broker: %s", token.Error())
	}

	return &client{
		client: pahoClient,
	}, nil
}

func (c *client) IsConnected() bool {
	return c.client.IsConnectionOpen()
}

func (c *client) HandleMessage(topic string, payload []byte) {
	message := Message{
		Timestamp: time.Now(),
		Value:     payload,
	}

	c.topics.AddMessage(topic, message)
}

func (c *client) GetTopic(reqPath string) (*Topic, bool) {
	return c.topics.Load(reqPath)
}

func (c *client) StoreTopic(t *Topic) {
	if t == nil {
		return
	}
	c.topics.Store(t)
}

func (c *client) Subscribe(t *Topic) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if exists := c.topics.HasSubscription(t.Path); exists {
		// There is already a subscription to this path,
		// so we shouldn't subscribe again.
		t.Active.Store(true)
		return nil
	}
	log.DefaultLogger.Debug("Subscribing to MQTT topic", "topic", t.Path)
	if token := c.client.Subscribe(t.Path, 0, func(_ paho.Client, m paho.Message) {
		// by wrapping HandleMessage we can directly get the correct topicPath for the incoming topic
		// and don't need to regex it against + and #.
		c.HandleMessage(t.Path, []byte(m.Payload()))
	}); token.Wait() && token.Error() != nil {
		log.DefaultLogger.Error("Error subscribing to MQTT topic", "topic", t.Path, "error", token.Error())
		return token.Error()
	}
	t.Active.Store(true)
	return nil
}

func (c *client) Unsubscribe(t *Topic) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.topics.Delete(t.Key())

	if exists := c.topics.HasSubscription(t.Path); exists {
		// There are still other subscriptions to this path,
		// so we shouldn't unsubscribe yet.
		return
	}

	log.DefaultLogger.Debug("Unsubscribing from MQTT topic", "topic", t.Path)

	if token := c.client.Unsubscribe(t.Path); token.Wait() && token.Error() != nil {
		log.DefaultLogger.Error("Error unsubscribing from MQTT topic", "topic", t.Path, "error", token.Error())
	}
}

func (c *client) Dispose() {
	log.DefaultLogger.Info("MQTT Disconnecting")
	c.client.Disconnect(250)
}
