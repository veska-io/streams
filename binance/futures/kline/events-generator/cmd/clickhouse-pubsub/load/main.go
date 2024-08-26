package main

import (
	connector "github.com/veska-io/streams-connectors/binance/futures/kline/events-generator/src"
)

func main() {
	connector.MustRun()
}
