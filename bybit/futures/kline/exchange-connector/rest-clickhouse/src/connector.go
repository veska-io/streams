package connector

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/hirokisan/bybit/v2"
	"github.com/veska-io/streams-connectors/bybit/futures/kline/exchange-connector/rest-clickhouse/src/consumer"
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

		klinesResponse, ok := response.Data.(*bybit.V5GetKlineResponse)
		if !ok {
			c.logger.Error("failed to cast response data to *futures.Kline")
			continue
		}

		c.logger.Debug("received klines", slog.Int("count", len(klinesResponse.Result.List)))

		for _, k := range klinesResponse.Result.List {

			startTimestamp, err := strconv.ParseInt(k.StartTime, 10, 64)
			if err != nil {
				c.logger.Error("failed to parse start timestamp", slog.String("err", err.Error()))
				continue
			}

			data := []any{
				response.Task.Market,
				response.Task.Market[:len(response.Task.Market)-4],
				response.Task.Market[len(response.Task.Market)-4:],

				time.UnixMilli(startTimestamp).Truncate(time.Hour).UnixMilli(),
				k.StartTime,

				k.Open,
				k.High,
				k.Low,
				k.Close,
				k.Volume,
				k.Turnover,

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
