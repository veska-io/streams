package kline_parser

import (
	"context"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"

	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/veska-io/streams-connectors/internal/aggregates/src/config"
	"github.com/veska-io/streams-connectors/internal/aggregates/src/connector"
	"github.com/veska-io/streams-connectors/internal/aggregates/src/logger"

	producer "github.com/veska-io/streams-connectors/producers/clickhouse"
)

func init() {
	functions.CloudEvent("RunMain", RunConnector)
}

func RunConnector(ctx context.Context, e event.Event) error {
	cfg := config.MustNew()

	logger := logger.New(cfg.Debug)
	logger.Debug("cfg: ", slog.Any("cfg", cfg))

	logger.Info("starting conector")

	ctx, cancel := context.WithCancel(context.Background())
	p, err := producer.New(ctx, logger,
		cfg.Clickhouse.Host, cfg.Clickhouse.Database, cfg.Clickhouse.User, cfg.Clickhouse.Password,
		cfg.Producer.Table, time.Duration(cfg.Producer.WriteIntervalSeconds)*time.Second,
	)
	if err != nil {
		panic(fmt.Errorf("unable to create the producer: %w", err))
	}

	connector, err := connector.New(ctx, logger, "binance-funding-rate-events-generator", "v0.0.1",
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

	return nil
}
