package config_test

import (
	"testing"

	"github.com/veska-io/streams-connectors/binance/futures/funding-rate/exchange-connector/rest-clickhouse/src/config"
)

func TestCOnfig(t *testing.T) {
	c := config.MustNew()

	t.Logf("Config: %+v", c)
}
