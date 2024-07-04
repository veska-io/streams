package main

import (
	"context"

	"github.com/veska-io/streams-connectors/binance/futures/kline/exchange-connector/pubsub-clickhouse/cmd/runner"
)

func main() {
	runner.MustRun(context.Background())
}
