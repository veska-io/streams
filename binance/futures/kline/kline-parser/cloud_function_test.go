package kline_parser_test

import (
	"context"
	"testing"

	"github.com/cloudevents/sdk-go/v2/event"
	kline_parser "github.com/veska-io/streams-connectors/binance/futures/kline/kline-parser"
	consumer "github.com/veska-io/streams-connectors/binance/futures/kline/kline-parser/src/consumer"
	binancepb "github.com/veska-io/streams-proto/gen/go/streams/binance/futures"
	"google.golang.org/protobuf/proto"
)

func TestRunConnector(t *testing.T) {
	exchangeEvent := &binancepb.Kline{
		OpenTime:                1620000000000,
		CloseTime:               1620000000000,
		Open:                    "1000",
		Close:                   "2000",
		High:                    "3000",
		Low:                     "4000",
		Volume:                  "5000",
		QuotAssetVolume:         "6000",
		TradeNum:                7,
		TakerBuyBaseAssetVolume: "8000",
		TakerBuyQuotAssetVolume: "9000",
		Symbol:                  "BTCUSDT",
		Base:                    "BTC",
		Quot:                    "USDT",
	}

	eventData, err := proto.Marshal(exchangeEvent)
	if err != nil {
		t.Errorf("failed to marshal event %v", err)
	}

	m := consumer.PubSubMessage{
		Data: eventData,
	}

	msg := consumer.MessagePublishedData{
		Message: m,
	}

	e := event.New()
	e.SetDataContentType("application/json")
	e.SetData(e.DataContentType(), msg)

	kline_parser.RunConnector(context.Background(), e)
}
