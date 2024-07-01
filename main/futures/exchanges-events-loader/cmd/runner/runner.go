package runner

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	connector "github.com/veska-io/streams-connectors/main/futures/exchanges-events-loader/src"
	"github.com/veska-io/streams-connectors/main/futures/exchanges-events-loader/src/config"
	"github.com/veska-io/streams-connectors/main/futures/exchanges-events-loader/src/logger"
)

func MustRun() {
	cfg := config.MustNew()
	logger := logger.New(cfg.Debug)

	logger.Info("starting conector")
	logger.Debug("config", slog.Any("cfg", cfg))

	ctx, cancel := context.WithCancel(context.Background())
	connector, err := connector.New(ctx, logger, cancel,
		cfg.PubSubProjectId, cfg.PubSubTopic, cfg.SubscriptionId,
		cfg.Host, cfg.Database, cfg.User, cfg.Password, cfg.Table,
		cfg.WriteIntervalSeconds, cfg.IdleTimeoutSeconds, cfg.MaxOutstandingMessages)
	if err != nil {
		logger.Error("failed to create connector", slog.String("err", err.Error()))
		return
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
