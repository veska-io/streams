package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/veska-io/streams-connectors/binance/futures/open-interest/events-generator/src/config"
	"github.com/veska-io/streams-connectors/binance/futures/open-interest/events-generator/src/connector"
	"github.com/veska-io/streams-connectors/binance/futures/open-interest/events-generator/src/logger"
	producer "github.com/veska-io/streams-connectors/binance/futures/open-interest/events-generator/src/producers/pubsub"
)

func main() {
	cfg := config.MustNew()

	logger := logger.New(cfg.Debug)
	logger.Debug("cfg: ", slog.Any("cfg", cfg))

	logger.Info("starting conector")

	ctx, cancel := context.WithCancel(context.Background())
	p, err := producer.New(ctx, logger, cfg.PubsubProducer.ProjectId, cfg.PubsubProducer.TopicId)
	if err != nil {
		panic(fmt.Errorf("unable to create the producer: %w", err))
	}

	connector, err := connector.New(ctx, logger, "binance-open-interest-events-generator", "v0.0.1",
		cfg.Clickhouse.Host, cfg.Clickhouse.Port, cfg.Clickhouse.Database,
		cfg.Clickhouse.User, cfg.Clickhouse.Password,
		cfg.Consumer.Start, cfg.Consumer.End,
		p,
	)

	if err != nil {
		panic(fmt.Errorf("unable to create the connector: %w", err))
	}

	go func() {
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

		s := <-stop
		logger.Info("stopping application", slog.String("signal", s.String()))
		cancel()
	}()

	startTime := time.Now()
	connector.Run()
	logger.Info("connector done", slog.Duration("duration", time.Since(startTime)))
}
