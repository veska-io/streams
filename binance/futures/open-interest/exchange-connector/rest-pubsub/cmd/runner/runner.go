package runner

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	connector "github.com/veska-io/streams-connectors/binance/futures/open-interest/exchange-connector/rest-pubsub/src"
	"github.com/veska-io/streams-connectors/binance/futures/open-interest/exchange-connector/rest-pubsub/src/config"
	"github.com/veska-io/streams-connectors/binance/futures/open-interest/exchange-connector/rest-pubsub/src/logger"
)

func MustRun(ctxGcp context.Context) {
	cfg := config.MustNew()
	logger := logger.New(cfg.Debug)

	logger.Info("starting conector")

	ctx, cancel := context.WithCancel(context.Background())
	connector := connector.New(ctx, logger,
		cfg.Consumer.Markets,
		cfg.Consumer.Start,
		cfg.Consumer.End,
		cfg.Consumer.Rps,
		time.Duration(cfg.Consumer.TaskQuantSeconds)*time.Second,
		cfg.Producer.ProjectId,
		cfg.Producer.TopicId,
	)

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
