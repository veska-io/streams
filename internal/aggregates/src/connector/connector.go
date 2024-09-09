package connector

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	consumer "github.com/veska-io/streams-connectors/consumers/clickhouse"
	local_consumer "github.com/veska-io/streams-connectors/internal/aggregates/src/consumer"

	producer "github.com/veska-io/streams-connectors/producers/clickhouse"
)

type Connector struct {
	consumer *consumer.Consumer
	producer *producer.Producer

	ctx    context.Context
	logger *slog.Logger
}

func New(ctx context.Context, logger *slog.Logger, name, version string,
	host string, port uint32, database, username, password string,
	fromDate, toDate time.Time,
	producer *producer.Producer,
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
		for range c.producer.StatusStream {
		}
		wg.Done()
	}()

	for row := range c.consumer.DataStream {
		c.logger.Debug("processing funding row")
		agg := local_consumer.Aggregate{}
		(*row).ScanStruct(&agg)

		c.producer.DataStream <- producer.Message{
			Data: []any{
				agg.AggTimestamp,
				agg.Exchange,
				agg.Market,
				agg.Base,
				agg.Quot,
				agg.PriceOpen,
				agg.PriceClose,
				agg.PriceHigh,
				agg.PriceLow,
				agg.VolumeBase,
				agg.VolumeQuot,
				agg.VolumeBaseBuyTaker,
				agg.VolumeQuotBuyTaker,
				agg.VolumeBaseSellTaker,
				agg.VolumeQuotSellTaker,
				agg.OiOpen,
				agg.TradesCount,
				agg.LiquidationsShortsCount,
				agg.LiquidationsLongsCount,
				agg.LiquidationsShortsBaseVolume,
				agg.LiquidationsLongsBaseVolume,
				agg.LiquidationsShortsQuotVolume,
				agg.LiquidationsLongsQuotVolume,
				agg.FundingRate,
				agg.FundingPrice,
				uint64(time.Now().UTC().UnixMilli()),
			},
		}
	}

	close(c.producer.DataStream)
	wg.Wait()
}
