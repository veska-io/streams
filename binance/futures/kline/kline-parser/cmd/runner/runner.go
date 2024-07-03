package runner

import (
	"context"
	"log/slog"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/veska-io/streams-connectors/binance/futures/kline/kline-parser/config"
	parser "github.com/veska-io/streams-connectors/binance/futures/kline/kline-parser/src"
	"github.com/veska-io/streams-connectors/binance/futures/kline/kline-parser/src/logger"
)

func MustRun(ctx context.Context, e event.Event) {
	cfg := config.MustNew()
	logger := logger.New(cfg.Debug)

	logger.Info("starting conector")

	connector, err := parser.New(ctx, logger,
		cfg.PubSubProjectId,
		cfg.PubSubTopic,
		e,
	)

	if err != nil {
		logger.Error("unable to create the connector", slog.String("error", err.Error()))
		return
	}

	startTime := time.Now()
	connector.Run()
	logger.Info("connector done", slog.Duration("duration", time.Since(startTime)))
}
