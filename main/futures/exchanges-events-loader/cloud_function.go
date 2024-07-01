package exchanges_events_loader

import (
	"context"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
	runner "github.com/veska-io/streams-connectors/main/futures/exchanges-events-loader/cmd/runner"
)

func init() {
	functions.CloudEvent("exchangesEventsLoader", runConnector)
}

func runConnector(ctx context.Context, e event.Event) error {
	runner.MustRun()

	return nil
}
