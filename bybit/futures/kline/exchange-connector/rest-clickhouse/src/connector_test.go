package connector_test

import (
	"context"
	"testing"

	"github.com/veska-io/streams-connectors/bybit/futures/kline/exchange-connector/rest-clickhouse/cmd/runner"
)

func TestRun(t *testing.T) {
	runner.MustRun(context.Background())
}
