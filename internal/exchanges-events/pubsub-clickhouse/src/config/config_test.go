package config_test

import (
	"testing"

	"github.com/veska-io/streams-connectors/internal/exchanges-events/pubsub-clickhouse/src/config"
)

func TestCOnfig(t *testing.T) {
	c := config.MustNew()

	t.Logf("Config: %+v", c)
}
