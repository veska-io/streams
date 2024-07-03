package parser

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"
	binancepb "github.com/veska-io/streams-proto/gen/go/streams/binance/futures"
	eeventspb "github.com/veska-io/streams-proto/gen/go/streams/main/futures"
	"google.golang.org/protobuf/proto"

	"github.com/veska-io/streams-connectors/binance/futures/kline/kline-parser/src/consumer"
	pub_sub "github.com/veska-io/streams-connectors/producers/pub-sub"
)

type Connector struct {
	exchange string

	consumer *consumer.Consumer
	producer *pub_sub.Producer

	ctx    context.Context
	logger *slog.Logger
}

func New(ctx context.Context, logger *slog.Logger,
	pubsubProjectId, pubsubTopic string, event event.Event,
) (*Connector, error) {

	c := consumer.New(ctx, logger, event)

	p, err := pub_sub.New(ctx, logger, pubsubProjectId, pubsubTopic)
	if err != nil {
		return nil, fmt.Errorf("unable to create producer: %w", err)
	}

	return &Connector{
		exchange: "binance",

		producer: p,
		consumer: c,

		ctx:    ctx,
		logger: logger,
	}, nil
}

func (c *Connector) Run() {
	go c.consumer.Run()
	go c.producer.Run()

	for kline := range c.consumer.ResponseChannel {

		events, err := ParseKline(kline)
		if err != nil {
			c.logger.Error("unable to parse the kline", slog.String("error", err.Error()))
			continue
		}

		for _, event := range events {
			msg, err := proto.Marshal(event)
			if err != nil {
				c.logger.Error("failed to marshal event", slog.String("err", err.Error()))
				continue
			}

			c.producer.DataStream <- pub_sub.Message{
				Id:   kline.GetOpenTime(),
				Data: msg,
			}
		}
	}

	close(c.producer.DataStream)
}

func ParseKline(kline *binancepb.Kline) ([]*eeventspb.Event, error) {
	priceEvent, err := parsePriceEvent(kline)
	if err != nil {
		return nil, fmt.Errorf("parsing priceEvent: %w", err)
	}

	volumeEvent, err := parseVolumeEvent(kline)
	if err != nil {
		return nil, fmt.Errorf("parsing volumeEvent: %w", err)
	}

	tradesEvent, err := parseTradesEvent(kline)
	if err != nil {
		return nil, fmt.Errorf("parsing tradesEvent: %w", err)
	}

	return []*eeventspb.Event{priceEvent, volumeEvent, tradesEvent}, nil
}

func parsePriceEvent(kline *binancepb.Kline) (*eeventspb.Event, error) {
	quoteAsset := "usd"
	baseAsset := strings.ToLower(kline.GetSymbol()[:len(kline.GetSymbol())-4])

	eventTimestamp := uint64(time.UnixMilli(kline.GetOpenTime()).Truncate(time.Hour).UnixMilli())

	priceOpen, err := strconv.ParseFloat(kline.GetOpen(), 64)
	if err != nil {
		return nil, fmt.Errorf("parsing priceOpen: %w", err)
	}

	priceClose, err := strconv.ParseFloat(kline.GetClose(), 64)
	if err != nil {
		return nil, fmt.Errorf("parsing priceClose: %w", err)
	}

	priceHigh, err := strconv.ParseFloat(kline.GetHigh(), 64)
	if err != nil {
		return nil, fmt.Errorf("parsing priceHigh: %w", err)
	}

	priceLow, err := strconv.ParseFloat(kline.GetLow(), 64)
	if err != nil {
		return nil, fmt.Errorf("parsing priceLow: %w", err)
	}

	priceEvent := eeventspb.Event{
		Event:          "price",
		EventTimestamp: eventTimestamp,

		Exchange: "binance",
		Market:   baseAsset + "-" + quoteAsset,
		Base:     baseAsset,
		Quot:     quoteAsset,

		PriceOpen:  &priceOpen,
		PriceClose: &priceClose,
		PriceHigh:  &priceHigh,
		PriceLow:   &priceLow,
	}

	return &priceEvent, nil
}

func parseVolumeEvent(kline *binancepb.Kline) (*eeventspb.Event, error) {
	quoteAsset := "usd"
	baseAsset := strings.ToLower(kline.GetSymbol()[:len(kline.GetSymbol())-4])

	eventTimestamp := uint64(time.UnixMilli(kline.GetOpenTime()).Truncate(time.Hour).UnixMilli())

	volume, err := strconv.ParseFloat(kline.GetVolume(), 64)
	if err != nil {
		return nil, fmt.Errorf("parsing volume: %w", err)
	}
	quotAssetVolume, err := strconv.ParseFloat(kline.GetQuotAssetVolume(), 64)
	if err != nil {
		return nil, fmt.Errorf("parsing quotAssetVolume: %w", err)
	}
	takerBuyBaseAssetVolume, err := strconv.ParseFloat(kline.GetTakerBuyBaseAssetVolume(), 64)
	if err != nil {
		return nil, fmt.Errorf("parsing takerBuyBaseAssetVolume: %w", err)
	}
	takerBuyQuotAssetVolume, err := strconv.ParseFloat(kline.GetTakerBuyQuotAssetVolume(), 64)
	if err != nil {
		return nil, fmt.Errorf("parsing takerBuyQuotAssetVolume: %w", err)
	}

	volumeBaseSellTaker := volume - takerBuyBaseAssetVolume
	volumeQuotSellTaker := quotAssetVolume - takerBuyQuotAssetVolume

	volumeEvent := eeventspb.Event{
		Event:          "price",
		EventTimestamp: eventTimestamp,

		Exchange: "binance",
		Market:   baseAsset + "-" + quoteAsset,
		Base:     baseAsset,
		Quot:     quoteAsset,

		VolumeBase:          &volume,
		VolumeQuot:          &quotAssetVolume,
		VolumeBaseBuyTaker:  &takerBuyBaseAssetVolume,
		VolumeQuotBuyTaker:  &takerBuyQuotAssetVolume,
		VolumeBaseSellTaker: &volumeBaseSellTaker,
		VolumeQuotSellTaker: &volumeQuotSellTaker,
	}

	return &volumeEvent, nil
}

func parseTradesEvent(kline *binancepb.Kline) (*eeventspb.Event, error) {
	quoteAsset := "usd"
	baseAsset := strings.ToLower(kline.GetSymbol()[:len(kline.GetSymbol())-4])

	eventTimestamp := uint64(time.UnixMilli(kline.GetOpenTime()).Truncate(time.Hour).UnixMilli())
	tradesCount := int32(kline.GetTradeNum())

	tradesEvent := eeventspb.Event{
		Event:          "price",
		EventTimestamp: eventTimestamp,

		Exchange: "binance",
		Market:   baseAsset + "-" + quoteAsset,
		Base:     baseAsset,
		Quot:     quoteAsset,

		TradesCount: &tradesCount,
	}

	return &tradesEvent, nil
}
