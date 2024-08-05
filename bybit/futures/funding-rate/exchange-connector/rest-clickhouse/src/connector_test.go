package connector_test

import (
	"testing"

	"github.com/veska-io/streams-connectors/bybit/futures/funding-rate/exchange-connector/rest-clickhouse/cmd/runner"
)

func TestRun(t *testing.T) {
	runner.MustRun()
}
