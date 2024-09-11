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
			Data: TransformEvent(event),
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

func TransformEvent(event *eeventspb.ExchangesEvent) []any {
	eventName := "price"

	chEvent := []any{
		event.EventTimestamp,
		event.Exchange,
		event.Market,
		event.Base,
		event.Quot,
	}

	if event.GetPrice() != nil {
		chEvent = append(chEvent,
			event.GetPrice().PriceOpen,
			event.GetPrice().PriceClose,
			event.GetPrice().PriceHigh,
			event.GetPrice().PriceLow,
		)
	} else {
		chEvent = append(chEvent, nil, nil, nil, nil)
	}

	if event.GetVolume() != nil {
		eventName = "volume"

		chEvent = append(chEvent,
			event.GetVolume().VolumeBase,
			event.GetVolume().VolumeQuot,
			event.GetVolume().VolumeBaseBuyTaker,
			event.GetVolume().VolumeQuotBuyTaker,
			event.GetVolume().VolumeBaseSellTaker,
			event.GetVolume().VolumeQuotSellTaker,
		)
	} else {
		chEvent = append(chEvent, nil, nil, nil, nil, nil, nil)
	}

	if event.GetOi() != nil {
		eventName = "oi"

		chEvent = append(chEvent, event.GetOi().OiOpen)
	} else {
		chEvent = append(chEvent, nil)
	}

	if event.GetTrades() != nil {
		eventName = "trades"

		chEvent = append(chEvent, event.GetTrades().Count)
	} else {
		chEvent = append(chEvent, nil)
	}

	if event.GetLiquidations() != nil {
		eventName = "liquidations"

		chEvent = append(chEvent,
			event.GetLiquidations().LiquidationsShortsCount,
			event.GetLiquidations().LiquidationsLongsCount,
			event.GetLiquidations().LiquidationsShortsBaseVolume,
			event.GetLiquidations().LiquidationsLongsBaseVolume,
			event.GetLiquidations().LiquidationsShortsQuotVolume,
			event.GetLiquidations().LiquidationsLongsQuotVolume,
		)
	} else {
		chEvent = append(chEvent, nil, nil, nil, nil, nil, nil)
	}

	if event.GetFundingRate() != nil {
		eventName = "funding_rate"

		chEvent = append(chEvent,
			event.GetFundingRate().FundingRate,
			event.GetFundingRate().FundingPrice,
		)
	} else {
		chEvent = append(chEvent, nil, nil)
	}

	chEvent = append(chEvent, time.Now().UTC().UnixMilli())
	chEvent = append([]any{eventName}, chEvent...)

	return chEvent
}
