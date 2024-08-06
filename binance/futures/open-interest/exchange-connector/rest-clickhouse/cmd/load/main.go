package main

import (
	connector "github.com/veska-io/streams-connectors/binance/futures/open-interest/exchange-connector/rest-clickhouse/src"
)

func main() {
	connector.MustRun()
}
