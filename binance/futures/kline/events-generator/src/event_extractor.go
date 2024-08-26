package connector

import (
	"fmt"
	"strconv"
	"time"

	local_consumer "github.com/veska-io/streams-connectors/binance/futures/kline/events-generator/src/consumer"
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
		Event:          "price",
		EventTimestamp: kline.KlineTimestamp,

		Exchange: "binance",
		Market:   fmt.Sprintf("%s-%s", kline.Base, "usd"),
		Base:     kline.Base,
		Quot:     "usd",

		PriceOpen:  &priceOpen,
		PriceHigh:  &priceHigh,
		PriceLow:   &priceLow,
		PriceClose: &priceClose,

		ProcessedTimestamp: uint64(time.Now().UTC().UnixMilli()),
	}

	return event, nil
}

func ExtractVolumeEvent(kline local_consumer.Kline) (*eeventspb.ExchangesEvent, error) {
	volumeBase, err := strconv.ParseFloat(kline.Volume, 64)
	if err != nil {
		return nil, fmt.Errorf("unable to parse volume: %w", err)
	}

	volumeQuot, err := strconv.ParseFloat(kline.QuotAssetVolume, 64)
	if err != nil {
		return nil, fmt.Errorf("unable to parse volume quot: %w", err)
	}

	volumeBaseBuyTaker, err := strconv.ParseFloat(kline.TakerBuyBaseAssetVolume, 64)
	if err != nil {
		return nil, fmt.Errorf("unable to parse volume base buy taker: %w", err)
	}

	volumeQuotBuyTaker, err := strconv.ParseFloat(kline.TakerBuyQuotAssetVolume, 64)
	if err != nil {
		return nil, fmt.Errorf("unable to parse volume quot buy taker: %w", err)
	}

	volumeBaseSellTaker := volumeBase - volumeBaseBuyTaker
	volumeQuotSellTaker := volumeQuot - volumeQuotBuyTaker

	event := &eeventspb.ExchangesEvent{
		Event:          "volume",
		EventTimestamp: kline.KlineTimestamp,

		Exchange: "binance",
		Market:   fmt.Sprintf("%s-%s", kline.Base, "usd"),
		Base:     kline.Base,
		Quot:     "usd",

		VolumeBase:          &volumeBase,
		VolumeQuot:          &volumeQuot,
		VolumeBaseBuyTaker:  &volumeBaseBuyTaker,
		VolumeQuotBuyTaker:  &volumeQuotBuyTaker,
		VolumeBaseSellTaker: &volumeBaseSellTaker,
		VolumeQuotSellTaker: &volumeQuotSellTaker,

		ProcessedTimestamp: uint64(time.Now().UTC().UnixMilli()),
	}

	return event, nil
}

func ExtractTradesEvent(kline local_consumer.Kline) (*eeventspb.ExchangesEvent, error) {
	tradeNum := kline.TradeNum

	event := &eeventspb.ExchangesEvent{
		Event:          "trades",
		EventTimestamp: kline.KlineTimestamp,

		Exchange: "binance",
		Market:   fmt.Sprintf("%s-%s", kline.Base, "usd"),
		Base:     kline.Base,
		Quot:     "usd",

		TradesCount: &tradeNum,

		ProcessedTimestamp: uint64(time.Now().UTC().UnixMilli()),
	}

	return event, nil
}
