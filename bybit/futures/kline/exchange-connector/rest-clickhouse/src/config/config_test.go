package config_test

import (
	"testing"

	"github.com/veska-io/streams-connectors/bybit/futures/kline/exchange-connector/rest-clickhouse/src/config"
)

func TestCOnfig(t *testing.T) {
	c := config.MustNew()

	t.Logf("Config: %+v", c)
}
