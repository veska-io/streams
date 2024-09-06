package main

import (
	"context"

	connector "github.com/veska-io/streams-connectors/internal/exchanges-events/src"
)

func main() {
	connector.MustRun(context.Background())
}
