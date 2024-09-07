package connector

import (
	"fmt"
	"strconv"
	"time"

	local_consumer "github.com/veska-io/streams-connectors/bybit/futures/kline/events-generator/src/consumer"
	eeventspb "github.com/veska-io/streams-proto/gen/go/streams"
)

func ExtractPriceEvent(kline local_consumer.Kline) (*eeventspb.ExchangesEvent, error) {
	priceOpen, err := strconv.ParseFloat(kline.Open, 64)
	if err != nil {
		return nil, fmt.Errorf("unable to parse price open: %w", err)
	}

	priceHigh, err := strconv.ParseFloat(kline.High, 64)
	if err != nil {
		return nil, fmt.Errorf("unable to parse price high: %w", err)
	}

	priceLow, err := strconv.ParseFloat(kline.Low, 64)
	if err != nil {
		return nil, fmt.Errorf("unable to parse price low: %w", err)
	}

	priceClose, err := strconv.ParseFloat(kline.Close, 64)
	if err != nil {
		return nil, fmt.Errorf("unable to parse price close: %w", err)
	}

	event := &eeventspb.ExchangesEvent{
		EventTimestamp: kline.KlineTimestamp,

		Exchange: "binance",
		Market:   fmt.Sprintf("%s-%s", kline.Base, "usd"),
		Base:     kline.Base,
		Quot:     "usd",

		Event: &eeventspb.ExchangesEvent_Price{
			Price: &eeventspb.ExchangesEvent_PriceEvent{
				PriceOpen:  &priceOpen,
				PriceHigh:  &priceHigh,
				PriceLow:   &priceLow,
				PriceClose: &priceClose,
			},
		},

		ProcessedTimestamp: uint64(time.Now().UTC().UnixMilli()),
	}

	return event, nil
}

func ExtractVolumeEvent(kline local_consumer.Kline) (*eeventspb.ExchangesEvent, error) {
	volumeBase, err := strconv.ParseFloat(kline.Volume, 64)
	if err != nil {
		return nil, fmt.Errorf("unable to parse volume: %w", err)
	}

	volumeQuot, err := strconv.ParseFloat(kline.Turnover, 64)
	if err != nil {
		return nil, fmt.Errorf("unable to parse volume quot: %w", err)
	}

	event := &eeventspb.ExchangesEvent{
		EventTimestamp: kline.KlineTimestamp,

		Exchange: "binance",
		Market:   fmt.Sprintf("%s-%s", kline.Base, "usd"),
		Base:     kline.Base,
		Quot:     "usd",

		Event: &eeventspb.ExchangesEvent_Volume{
			Volume: &eeventspb.ExchangesEvent_VolumeEvent{
				VolumeBase: &volumeBase,
				VolumeQuot: &volumeQuot,
			},
		},

		ProcessedTimestamp: uint64(time.Now().UTC().UnixMilli()),
	}

	return event, nil
}
