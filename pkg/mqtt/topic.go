package mqtt

import (
	"crypto/sha1"
	"encoding/base64"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/data"
)

type Message struct {
	Timestamp time.Time
	Value     []byte
}

// Topic represents a MQTT topic.
type Topic struct {
	Path       string `json:"topic"`
	Downsample bool   `json:"downsample"`
	Interval   time.Duration
	Messages   []Message
	Active     atomic.Bool
	framer     *framer
	mu         sync.Mutex
}

// Key returns the key for the topic.
// The key is a combination of the interval string and the path.
// For example, if the path is "my/topic" and the interval is 1s, the key will be "1s/my/topic".
func (t *Topic) Key() string {
	hash := sha1.Sum([]byte(path.Join(strconv.FormatBool(t.Downsample), t.Interval.String(), t.Path)))
	return base64.URLEncoding.EncodeToString(hash[:])
}

// ToDataFrame converts the topic to a data frame.
func (t *Topic) ToDataFrame() (*data.Frame, error) {
	if t.framer == nil {
		t.framer = newFramer()
	}
	m := []Message{}
	t.mu.Lock()
	t.Messages, m = m, t.Messages
	t.mu.Unlock()
	if t.Downsample && len(m) > 0 {
		return t.framer.toFrame(m[len(m)-1:])
	}
	return t.framer.toFrame(m)
}

func (t *Topic) AddMessage(message Message) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Messages = append(t.Messages, message)
}

// TopicMap is a thread-safe map of topics
type TopicMap struct {
	sync.Map
}

// Load returns the topic for the given topic key.
func (tm *TopicMap) Load(key string) (*Topic, bool) {
	t, ok := tm.Map.Load(key)
	if !ok {
		return nil, false
	}

	topic, ok := t.(*Topic)
	return topic, ok
}

// AddMessage adds a message to the topic for the given path.
func (tm *TopicMap) AddMessage(path string, message Message) {
	tm.Map.Range(func(key, t any) bool {
		topic, ok := t.(*Topic)
		if !ok {
			return false
		}
		if topic.Path == path {
			topic.AddMessage(message)
		}
		return true
	})
}

// HasSubscription returns true if the topic map has a subscription for the given path.
func (tm *TopicMap) HasSubscription(path string) bool {
	found := false

	tm.Map.Range(func(key, t any) bool {
		topic, ok := t.(*Topic)
		if !ok {
			return true // this shouldn't happen, but continue iterating
		}

		if topic.Active.Load() && topic.Path == path {
			found = true
			return false // topic found, stop iterating
		}

		return true // continue iterating
	})

	return found
}

// Store stores the topic in the map.
func (tm *TopicMap) Store(t *Topic) {
	tm.Map.LoadOrStore(t.Key(), t)
}

// Delete deletes the topic for the given key.
func (tm *TopicMap) Delete(key string) {
	tm.Map.Delete(key)
}
