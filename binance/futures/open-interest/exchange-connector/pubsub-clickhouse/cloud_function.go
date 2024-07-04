package kline_parser

import (
	"context"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"

	"github.com/veska-io/streams-connectors/binance/futures/open-interest/exchange-connector/pubsub-clickhouse/cmd/runner"
)

func init() {
	functions.CloudEvent("binanceKlineParser", RunConnector)
}

func RunConnector(ctx context.Context, e event.Event) error {
	runner.MustRun(ctx)

	return nil
}
