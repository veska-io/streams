package main

import (
	"context"

	"github.com/veska-io/streams-connectors/binance/futures/open-interest/exchange-connector/pubsub-clickhouse/cmd/runner"
)

func main() {
	runner.MustRun(context.Background())
}
