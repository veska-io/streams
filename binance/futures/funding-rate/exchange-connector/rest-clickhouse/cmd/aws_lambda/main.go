package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	connector "github.com/veska-io/streams-connectors/binance/futures/funding-rate/exchange-connector/rest-clickhouse/src"
)

func main() {
	lambda.Start(connector.MustRun)
}
