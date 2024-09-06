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
	chEvent := []any{
		event.EventTimestamp,
		event.Exchange,
		event.Market,
		event.Base,
		event.Quot,
	}

	if event.GetPrice() != nil {
		chEvent = append(chEvent,
			event.GetPrice().GetPriceOpen(),
			event.GetPrice().GetPriceClose(),
			event.GetPrice().GetPriceHigh(),
			event.GetPrice().GetPriceLow(),
		)
	} else {
		chEvent = append(chEvent, nil, nil, nil, nil)
	}

	if event.GetVolume() != nil {
		chEvent = append(chEvent,
			event.GetVolume().GetVolumeBase(),
			event.GetVolume().GetVolumeQuot(),
			event.GetVolume().GetVolumeBaseBuyTaker(),
			event.GetVolume().GetVolumeQuotBuyTaker(),
			event.GetVolume().GetVolumeBaseSellTaker(),
			event.GetVolume().GetVolumeQuotSellTaker(),
		)
	} else {
		chEvent = append(chEvent, nil, nil, nil, nil, nil, nil)
	}

	if event.GetOi() != nil {
		chEvent = append(chEvent, event.GetOi().GetOiOpen())
	} else {
		chEvent = append(chEvent, nil)
	}

	if event.GetTrades() != nil {
		chEvent = append(chEvent, event.GetTrades().GetCount())
	} else {
		chEvent = append(chEvent, nil)
	}

	if event.GetLiquidations() != nil {
		chEvent = append(chEvent,
			event.GetLiquidations().GetLiquidationsShortsCount(),
			event.GetLiquidations().GetLiquidationsLongsCount(),
			event.GetLiquidations().GetLiquidationsShortsBaseVolume(),
			event.GetLiquidations().GetLiquidationsLongsBaseVolume(),
			event.GetLiquidations().GetLiquidationsShortsQuotVolume(),
			event.GetLiquidations().GetLiquidationsLongsQuotVolume(),
		)
	} else {
		chEvent = append(chEvent, nil, nil, nil, nil, nil, nil)
	}

	if event.GetFundingRate() != nil {
		chEvent = append(chEvent,
			event.GetFundingRate().GetFundingRate(),
			event.GetFundingRate().GetFundingPrice(),
		)
	} else {
		chEvent = append(chEvent, nil, nil)
	}

	chEvent = append(chEvent, time.Now().UTC().UnixMilli())

	return chEvent
}
