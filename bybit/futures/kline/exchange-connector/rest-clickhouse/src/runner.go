package connector

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/veska-io/streams-connectors/bybit/futures/kline/exchange-connector/rest-clickhouse/src/logger"

	"github.com/veska-io/streams-connectors/bybit/futures/kline/exchange-connector/rest-clickhouse/src/config"
)

func MustRun(ctxGcp context.Context) {
	cfg := config.MustNew()
	logger := logger.New(cfg.Debug)
	logger.Debug("cfg: ", slog.Any("cfg", cfg))

	logger.Info("starting conector")

	ctx, cancel := context.WithCancel(context.Background())
	connector, err := New(ctx, logger,
		cfg.Consumer.Markets, cfg.Consumer.Start, cfg.Consumer.End,
		cfg.Consumer.Rps, time.Duration(cfg.Consumer.TaskQuantSeconds)*time.Second,
		cfg.Producer.Host, cfg.Producer.Database, cfg.Producer.User, cfg.Producer.Password,
		cfg.Producer.Table, cfg.Producer.WriteIntervalSeconds,
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
