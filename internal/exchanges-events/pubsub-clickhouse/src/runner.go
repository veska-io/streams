package connector

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/veska-io/streams-connectors/internal/exchanges-events/pubsub-clickhouse/src/config"
	"github.com/veska-io/streams-connectors/internal/exchanges-events/pubsub-clickhouse/src/logger"
)

func MustRun(ctxGcp context.Context) {
	cfg := config.MustNew()
	logger := logger.New(cfg.Debug)

	logger.Info("starting conector")
	logger.Debug("config", slog.Any("cfg", cfg))

	ctx, cancel := context.WithCancel(context.Background())
	connector, err := New(ctx, logger, cancel,
		cfg.Consumer.ProjectId, cfg.Consumer.TopicId, cfg.Consumer.SubscriptionId,
		cfg.Consumer.MaxOutstandingMessages,
		cfg.Producer.Host, cfg.Producer.Database, cfg.Producer.User, cfg.Producer.Password,
		cfg.Producer.Table, cfg.Producer.WriteIntervalSeconds, cfg.IdleTimeoutSeconds)
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

	go func() {
		for {
			select {
			case <-ctxGcp.Done():
				cancel()
				return
			}

		}
	}()

	startTime := time.Now()
	connector.Run()
	logger.Info("connector done", slog.Duration("duration", time.Since(startTime)))
}
