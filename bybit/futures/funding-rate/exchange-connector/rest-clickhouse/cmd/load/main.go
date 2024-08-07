package main

import (
	connector "github.com/veska-io/streams-connectors/bybit/futures/funding-rate/exchange-connector/rest-clickhouse/src"
)

func main() {
	connector.MustRun()
}
