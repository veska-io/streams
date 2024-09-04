package producer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/veska-io/streams-connectors/producers/clickhouse"
	eeventspb "github.com/veska-io/streams-proto/gen/go/streams"
)

type Producer struct {
	DataStream   chan *eeventspb.ExchangesEvent
	StatusStream chan any

	chProducer *clickhouse.Producer

	host     string
	database string
	username string
	password string
	table    string

	ctx    context.Context
	logger *slog.Logger
}

func New(ctx context.Context, logger *slog.Logger,
	host, database, username, password, table string, writeInterval time.Duration,
) (*Producer, error) {
	p, err := clickhouse.New(ctx, logger,
		host, database, username, password, table, writeInterval)

	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub producer: %w", err)
	}

	return &Producer{
		DataStream:   make(chan *eeventspb.ExchangesEvent),
		StatusStream: make(chan any),
		chProducer:   p,

		ctx:    ctx,
		logger: logger,
	}, nil
}

func (p *Producer) Run() {
	wg := sync.WaitGroup{}

	go p.chProducer.Run()

	go func() {
		for status := range p.chProducer.StatusStream {
			p.StatusStream <- status
		}

		close(p.StatusStream)
	}()

	for event := range p.DataStream {
		p.chProducer.DataStream <- clickhouse.Message{
			Data: []any{
				event.Event,
				event.EventTimestamp,
				event.Exchange,
				event.Market,
				event.Base,
				event.Quot,
				event.PriceOpen,
				event.PriceClose,
				event.PriceHigh,
				event.PriceLow,
				event.VolumeBase,
				event.VolumeQuot,
				event.VolumeBaseBuyTaker,
				event.VolumeQuotBuyTaker,
				event.VolumeBaseSellTaker,
				event.VolumeQuotSellTaker,
				event.OiOpen,
				event.TradesCount,
				event.LiquidationsShortsCount,
				event.LiquidationsLongsCount,
				event.LiquidationsShortsBaseVolume,
				event.LiquidationsLongsBaseVolume,
				event.LiquidationsShortsQuotVolume,
				event.LiquidationsLongsQuotVolume,
				event.FundingRate,
				time.Now().UTC().UnixMilli(),
			},
		}
	}

	close(p.chProducer.DataStream)
	wg.Wait()
}

func (p *Producer) GetDataStream() chan<- *eeventspb.ExchangesEvent {
	return p.DataStream
}

func (p *Producer) GetStatusStream() <-chan any {
	return p.StatusStream
}
