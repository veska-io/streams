package connector

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	local_consumer "github.com/veska-io/streams-connectors/binance/futures/kline/events-generator/src/consumer"
	consumer "github.com/veska-io/streams-connectors/consumers/clickhouse"
	eeventspb "github.com/veska-io/streams-proto/gen/go/streams"
)

type Connector struct {
	consumer *consumer.Consumer
	producer Producer

	ctx    context.Context
	logger *slog.Logger
}

type Producer interface {
	Run()
	GetDataStream() chan<- *eeventspb.ExchangesEvent
	GetStatusStream() <-chan any
}

func New(ctx context.Context, logger *slog.Logger, name, version string,
	host string, port uint32, database, username, password string,
	fromDate, toDate time.Time,
	producer Producer,
) (*Connector, error) {
	c, err := local_consumer.New(
		ctx, logger, name, version,
		host, port, database, username, password,
		fromDate, toDate,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	return &Connector{
		consumer: c,
		producer: producer,

		ctx:    ctx,
		logger: logger,
	}, nil
}

func (c *Connector) Run() {
	wg := sync.WaitGroup{}

	go c.consumer.Run()
	go c.producer.Run()

	wg.Add(1)
	go func() {
		statusStream := c.producer.GetStatusStream()
		for range statusStream {
			c.logger.Info("producer status")
		}
		wg.Done()
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
			c.producer.GetDataStream() <- event
		}
	}

	close(c.producer.GetDataStream())
	wg.Wait()
}
