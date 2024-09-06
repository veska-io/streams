package kline_parser

import (
	"context"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"

	connector "github.com/veska-io/streams-connectors/internal/exchanges-events/src"
)

func init() {
	functions.CloudEvent("RunMain", RunConnector)
}

// RunConnector ...
func RunConnector(ctx context.Context, e event.Event) error {
	connector.MustRun(ctx)

	return nil
}
