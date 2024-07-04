package exchange_connector

import (
	"context"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
	runner "github.com/veska-io/streams-connectors/binance/futures/kline/exchange-connector/rest-pubsub/cmd/runner"
)

func init() {
	functions.CloudEvent("binanceKlineExchangeConnector", runConnector)
}

func runConnector(ctx context.Context, e event.Event) error {
	runner.MustRun(ctx)

	return nil
}
