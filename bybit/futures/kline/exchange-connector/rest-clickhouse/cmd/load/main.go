package main

import (
	"context"

	connector "github.com/veska-io/streams-connectors/bybit/futures/kline/exchange-connector/rest-clickhouse/src"
)

func main() {
	connector.MustRun(context.Background())
}
