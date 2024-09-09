package consumer_test

import (
	"context"
	"testing"
	"time"

	"github.com/adshao/go-binance/v2"
	connector "github.com/veska-io/streams-connectors/binance/futures/funding-rate/exchange-connector/rest-clickhouse/src"
)

func TestStrangeRequest(t *testing.T) {
	start := time.Date(2024, 9, 9, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 9, 9, 11, 0, 0, 0, time.UTC)

	restClient := binance.NewFuturesClient("", "")
	fRate, err := restClient.NewFundingRateHistoryService().Symbol("DASHUSDT").Limit(1000).
		StartTime(start.UnixMilli()).EndTime(end.UnixMilli()).Do(context.Background())

	if err != nil {
		t.Error(err)
	}

	t.Log(fRate[0])
}

func TestRun(t *testing.T) {
	connector.MustRun()
}
