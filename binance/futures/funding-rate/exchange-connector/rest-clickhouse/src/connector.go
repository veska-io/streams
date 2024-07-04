package connector

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/adshao/go-binance/v2/futures"
	"github.com/veska-io/streams-connectors/binance/futures/funding-rate/exchange-connector/rest-clickhouse/src/consumer"
	chprd "github.com/veska-io/streams-connectors/producers/clickhouse"
)

type Connector struct {
	consumer *consumer.Consumer
	producer *chprd.Producer

	ctx    context.Context
	logger *slog.Logger
}

func New(ctx context.Context, logger *slog.Logger,
	markets []string, start, end time.Time, rps uint8, taskQuant time.Duration,
	chHost, chDatabase, chUser, chPassword, chTable string,
	writeInterval uint8,
) (*Connector, error) {
	c := consumer.New(ctx, logger,
		markets,
		start,
		end,
		rps,
		taskQuant,
	)

	p, err := chprd.New(ctx, logger,
		chHost, chDatabase, chUser, chPassword, chTable, time.Duration(writeInterval)*time.Second)
	if err != nil {
		logger.Error("failed to create clickhouse producer", slog.String("err", err.Error()))
		return nil, fmt.Errorf("failed to create clickhouse producer: %w", err)
	}

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
		for range c.producer.StatusStream {
		}
	}()

	statusCounter := 0

	for response := range c.consumer.ResponseChan {
		if statusCounter == 0 {
			c.logger.Info("precent completed", slog.Uint64("status", uint64(c.consumer.Status())))
		}

		fRates, ok := response.Data.([]*futures.FundingRateHistory)
		if !ok {
			c.logger.Error("failed to cast response data to FundingRateHistory")
			continue
		}

		for _, fRate := range fRates {
			data := []any{
				response.Task.Market,
				response.Task.Market[:len(response.Task.Market)-4],
				response.Task.Market[len(response.Task.Market)-4:],

				uint64(fRate.FundingTime),
				fRate.FundingRate,
				fRate.MarkPrice,

				time.Now().UnixMilli(),
			}

			c.producer.DataStream <- chprd.Message{
				Data: data,
			}
		}

		statusCounter += 1
		statusCounter = statusCounter % 10
	}

	close(c.producer.DataStream)
}
