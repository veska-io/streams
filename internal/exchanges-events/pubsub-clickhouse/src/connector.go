package connector

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	pub_sub "github.com/veska-io/streams-connectors/consumers/pub-sub"
	chprd "github.com/veska-io/streams-connectors/producers/clickhouse"
	eeventspb "github.com/veska-io/streams-proto/gen/go/streams"

	"google.golang.org/protobuf/proto"
)

type Connector struct {
	consumer *pub_sub.Consumer
	producer *chprd.Producer

	idleTimeout          time.Duration
	lastMessageTimestamp atomic.Int64

	ctx    context.Context
	logger *slog.Logger
	cancel context.CancelFunc
}

func New(ctx context.Context, logger *slog.Logger, cancel context.CancelFunc,
	projectId, topicId, subscriptionId string,
	maxOutstandingMessages int,
	chHost, chDatabase, chUser, chPassword, chTable string,
	writeInterval uint8, idleTimeoutSeconds uint8,
) (*Connector, error) {
	consumer, err := pub_sub.New(ctx, logger,
		projectId, topicId, subscriptionId, maxOutstandingMessages)
	if err != nil {
		logger.Error("failed to create pubsub consumer", slog.String("err", err.Error()))
		return nil, fmt.Errorf("failed to create pubsub consumer: %w", err)
	}

	producer, err := chprd.New(ctx, logger,
		chHost, chDatabase, chUser, chPassword, chTable, time.Duration(writeInterval)*time.Second)
	if err != nil {
		logger.Error("failed to create clickhouse producer", slog.String("err", err.Error()))
		return nil, fmt.Errorf("failed to create clickhouse producer: %w", err)
	}

	return &Connector{
		consumer: consumer,
		producer: producer,

		idleTimeout: time.Duration(idleTimeoutSeconds) * time.Second,

		ctx:    ctx,
		logger: logger,
		cancel: cancel,
	}, nil
}

func (c *Connector) Run() {
	var waitProducer sync.WaitGroup

	go c.consumer.Run()
	go c.producer.Run()

	waitProducer.Add(1)
	go func() {
		for pMsgs := range c.producer.StatusStream {
			for _, pMsg := range pMsgs {
				msg := pMsg.Meta.(*pubsub.Message)
				if pMsg.Err != nil {
					msg.Nack()
				} else {
					msg.Ack()
				}
			}
		}
		waitProducer.Done()
	}()

	go func() {
		for {
			nanoSince := time.Now().UnixNano() - c.lastMessageTimestamp.Load()
			if nanoSince > int64(c.idleTimeout) {
				c.logger.Info("idle timeout reached, stopping application")
				c.cancel()
				return
			}

			time.Sleep(time.Second)
		}
	}()

	c.lastMessageTimestamp.Store(time.Now().UnixNano())
	for msg := range c.consumer.DataStream {
		c.lastMessageTimestamp.Store(time.Now().UnixNano())
		c.logger.Debug("received message", slog.String("id", msg.ID))

		event := &eeventspb.ExchangesEvent{}
		if err := proto.Unmarshal(msg.Data, event); err != nil {
			c.logger.Error("failed to unmarshal trade message", slog.String("err", err.Error()))
			continue
		}

		data := []any{}

		c.producer.DataStream <- chprd.Message{
			Data: data,
			Meta: msg,
		}
	}

	close(c.producer.DataStream)
	waitProducer.Wait()
}
