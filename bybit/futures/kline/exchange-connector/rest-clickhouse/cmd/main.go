package main

import (
	"context"

	"github.com/veska-io/streams-connectors/bybit/futures/kline/exchange-connector/rest-clickhouse/cmd/runner"
)

func main() {
	runner.MustRun(context.Background())
}
