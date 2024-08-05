package consumer_test

import (
	"context"
	"testing"
	"time"

	"github.com/adshao/go-binance/v2"
	connector "github.com/veska-io/streams-connectors/binance/futures/funding-rate/exchange-connector/rest-clickhouse/src"
)

func TestStrangeRequest(t *testing.T) {
	start := time.Date(2020, 1, 1, 1, 0, 0, 0, time.UTC)
	end := time.Date(2021, 10, 20, 1, 0, 0, 0, time.UTC)

	restClient := binance.NewFuturesClient("", "")
	fRate, err := restClient.NewFundingRateHistoryService().Symbol("OXTUSDT").Limit(1000).
		StartTime(start.UnixMilli()).EndTime(end.UnixMilli()).Do(context.Background())

	if err != nil {
		t.Error(err)
	}

	t.Log(len(fRate))
}

func TestRun(t *testing.T) {
	connector.MustRun()
}
