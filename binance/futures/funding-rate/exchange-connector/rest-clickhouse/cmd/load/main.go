package main

import (
	connector "github.com/veska-io/streams-connectors/binance/futures/funding-rate/exchange-connector/rest-clickhouse/src"
)

func main() {
	connector.MustRun()
}
