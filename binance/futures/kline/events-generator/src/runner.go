package connector

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/veska-io/streams-connectors/binance/futures/kline/events-generator/src/config"
	"github.com/veska-io/streams-connectors/binance/futures/kline/events-generator/src/logger"
)

func MustRun() {
	cfg := config.MustNew()

	logger := logger.New(cfg.Debug)
	logger.Debug("cfg: ", slog.Any("cfg", cfg))

	logger.Info("starting conector")

	ctx, cancel := context.WithCancel(context.Background())

	connector, err := New(ctx, logger, "binance-klines-events-generator", "v0.0.1",
		cfg.Consumer.Host, cfg.Consumer.Port, cfg.Consumer.Database,
		cfg.Consumer.User, cfg.Consumer.Password,
		cfg.Consumer.Start, cfg.Consumer.End,
		cfg.Producer.ProjectId, cfg.Producer.TopicId,
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
