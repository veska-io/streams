package connector

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	local_consumer "github.com/veska-io/streams-connectors/binance/futures/kline/events-generator/src/consumer"
	consumer "github.com/veska-io/streams-connectors/consumers/clickhouse"
	pub_sub "github.com/veska-io/streams-connectors/producers/pub-sub"
	eeventspb "github.com/veska-io/streams-proto/gen/go/streams"
	"google.golang.org/protobuf/proto"
)

type Connector struct {
	consumer *consumer.Consumer
	producer *pub_sub.Producer

	ctx    context.Context
	logger *slog.Logger
}

func New(ctx context.Context, logger *slog.Logger, name, version string,
	host string, port uint32, database, username, password string,
	fromDate, toDate time.Time,
	pubsubProjectId, pubsubTopic string,
) (*Connector, error) {
	c, err := local_consumer.New(
		ctx, logger, name, version,
		host, port, database, username, password,
		fromDate, toDate,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	p, _ := pub_sub.New(ctx, logger,
		pubsubProjectId, pubsubTopic,
	)

	return &Connector{
		consumer: c,
		producer: p,

		ctx:    ctx,
		logger: logger,
	}, nil
}

func (c *Connector) Run() {
	go c.consumer.Run()
	go c.producer.Run()

	go func() {
		for msg := range c.producer.StatusStream {
			if msg.Error != nil {
				c.logger.Error("producer status", slog.String("error", msg.Error.Error()))
				continue
			}
		}
	}()

	for row := range c.consumer.DataStream {
		events := []*eeventspb.ExchangesEvent{}

		kline := local_consumer.Kline{}
		(*row).ScanStruct(&kline)

		priceEvent, err := ExtractPriceEvent(kline)
		if err != nil {
			c.logger.Error("failed to extract price event", slog.String("err", err.Error()))
		} else {
			events = append(events, priceEvent)
		}

		volumeEvent, err := ExtractVolumeEvent(kline)
		if err != nil {
			c.logger.Error("failed to extract volume event", slog.String("err", err.Error()))
		} else {
			events = append(events, volumeEvent)
		}

		tradesEvent, err := ExtractTradesEvent(kline)
		if err != nil {
			c.logger.Error("failed to extract trades event", slog.String("err", err.Error()))
		} else {
			events = append(events, tradesEvent)
		}

		for _, event := range events {
			msg, err := proto.Marshal(event)
			if err != nil {
				c.logger.Error("failed to marshal event", slog.String("err", err.Error()))
			}

			c.producer.DataStream <- pub_sub.Message{
				Data: msg,
			}
		}
	}

	close(c.producer.DataStream)
}
