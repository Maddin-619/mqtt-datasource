package plugin

import (
	"context"
	"fmt"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

func (ds *MQTTDatasource) RunStream(ctx context.Context, req *backend.RunStreamRequest, sender *backend.StreamSender) error {
	log.DefaultLogger.Debug("RunStream", "path", req.Path)
	// No more than one stream with the same ID may exist at the same time.
	topic, ok := ds.Client.GetTopic(req.Path)
	if !ok {
		err := fmt.Errorf("topic not found %s", req.Path)
		log.DefaultLogger.Error(err.Error())
		return err
	}
	if topic.Active.Load() {
		err := fmt.Errorf("stream already running %s", req.Path)
		log.DefaultLogger.Error(err.Error())
		return err
	}

	err := ds.Client.Subscribe(topic)
	if err != nil {
		return err
	}
	defer ds.Client.Unsubscribe(topic)

	ticker := time.NewTicker(topic.Interval)

	for {
		select {
		case <-ctx.Done():
			log.DefaultLogger.Debug("stopped streaming (context canceled)", "path", req.Path)
			ticker.Stop()
			return nil
		case <-ticker.C:
			frame, err := topic.ToDataFrame()
			if err != nil {
				log.DefaultLogger.Error("failed to convert topic to data frame", "path", req.Path, "error", err)
				break
			}
			if err := sender.SendFrame(frame, data.IncludeAll); err != nil {
				log.DefaultLogger.Error("failed to send data frame", "path", req.Path, "error", err)
			}

		}
	}
}

func (ds *MQTTDatasource) SubscribeStream(_ context.Context, req *backend.SubscribeStreamRequest) (*backend.SubscribeStreamResponse, error) {
	return &backend.SubscribeStreamResponse{
		Status: backend.SubscribeStreamStatusOK,
	}, nil
}

func (ds *MQTTDatasource) PublishStream(_ context.Context, _ *backend.PublishStreamRequest) (*backend.PublishStreamResponse, error) {
	return &backend.PublishStreamResponse{
		Status: backend.PublishStreamStatusPermissionDenied,
	}, nil
}
