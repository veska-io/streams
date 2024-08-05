package connector_test

import (
	"context"
	"testing"
	"time"

	"github.com/adshao/go-binance/v2/futures"
	"github.com/veska-io/streams-connectors/binance/futures/kline/exchange-connector/rest-clickhouse/src/logger"
	"github.com/veska-io/streams-connectors/binance/futures/kline/exchange-connector/rest-pubsub/src/consumer"

	connector "github.com/veska-io/streams-connectors/binance/futures/kline/exchange-connector/rest-clickhouse/src"
)

func TestConsumer(t *testing.T) {
	logger := logger.New(true)

	now := time.Now().UTC()
	end := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, time.UTC)
	start := end.Add(-1 * time.Duration(time.Hour) * 24 * 365 * 4)

	c := consumer.New(context.Background(), logger,
		[]string{"BTCUSDT"},
		start,
		end,
		7,
		60*60*1000,
	)

	go c.Run()

	for response := range c.ResponseChan {
		klinesResponse, ok := response.Data.([]*futures.Kline)
		if !ok {
			t.Error("response data is not of type []*futures.Kline")
			continue
		}

		t.Log(klinesResponse[len(klinesResponse)-1].OpenTime)
		t.Log(klinesResponse[len(klinesResponse)-1].OpenTime - klinesResponse[0].OpenTime)
	}
}

func TestRun(t *testing.T) {
	connector.MustRun(context.Background())
}
