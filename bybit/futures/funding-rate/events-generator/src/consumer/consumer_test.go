package consumer_test

import (
	"context"
	"testing"

	config "github.com/veska-io/streams-connectors/bybit/futures/funding-rate/events-generator/src/config"
	local_consumer "github.com/veska-io/streams-connectors/bybit/futures/funding-rate/events-generator/src/consumer"
	logger "github.com/veska-io/streams-connectors/bybit/futures/funding-rate/events-generator/src/logger"
)

func TestConsumer(t *testing.T) {
	log := logger.New(false)
	cfg := config.MustNew()

	c, err := local_consumer.New(
		context.Background(), log, "test", "v1",
		cfg.Clickhouse.Host, cfg.Clickhouse.Port, cfg.Clickhouse.Database,
		cfg.Clickhouse.User, cfg.Clickhouse.Password,
		cfg.Consumer.Start, cfg.Consumer.End,
	)

	if err != nil {
		t.Errorf("failed to create consumer: %v", err)
	}

	go c.Run()

	for row := range c.DataStream {
		kline := local_consumer.Funding{}
		(*row).ScanStruct(&kline)

		t.Logf("Kline: %+v", kline)
	}
}
