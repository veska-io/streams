package main

import (
	"context"

	connector "github.com/veska-io/streams-connectors/internal/exchanges-events/pubsub-clickhouse/src"
)

func main() {
	connector.MustRun(context.Background())
}
