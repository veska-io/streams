package consumer_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/adshao/go-binance/v2/futures"
	"github.com/veska-io/streams-connectors/binance/futures/kline/exchange-connector/src/consumer"
	binancepb "github.com/veska-io/streams-proto/gen/go/streams/binance/futures"
)

func TestConsumer(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	markets := []string{"BTCUSDT"}
	now := time.Now().UTC()

	end := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, time.UTC)
	start := end.Add(-24 * 30 * time.Duration(time.Hour))
	t.Logf("Start: %s, End: %s", start, end)

	c := consumer.New(ctx, logger,
		markets,
		start,
		end,
		10,
		60*60*1000*time.Second,
	)

	go c.Run()

	for msg := range c.ResponseChan {
		klines := msg.Data.([]*futures.Kline)
		for _, k := range klines {
			kline := binancepb.Kline{
				OpenTime: k.OpenTime,
				// Open:                    k.Open,
				// High:                    k.High,
				// Low:                     k.Low,
				// Close:                   k.Close,
				// Volume:                  k.Volume,
				// CloseTime:               k.CloseTime,
				// QuotAssetVolume:         k.QuoteAssetVolume,
				// TradeNum:                k.TradeNum,
				// TakerBuyBaseAssetVolume: k.TakerBuyBaseAssetVolume,
				// TakerBuyQuotAssetVolume: k.TakerBuyQuoteAssetVolume,
			}
			t.Log(kline.OpenTime)
		}
	}
}
