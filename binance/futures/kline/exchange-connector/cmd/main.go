package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/adshao/go-binance/v2"
	connector "github.com/veska-io/streams-connectors/binance/futures/kline/exchange-connector/src"
	"github.com/veska-io/streams-connectors/binance/futures/kline/exchange-connector/src/config"
	"github.com/veska-io/streams-connectors/binance/futures/kline/exchange-connector/src/logger"
)

func main() {
	cfg := config.MustNew()
	if len(cfg.Markets) == 0 {
		populateMarkets(cfg)
	}
	logger := logger.New(cfg.Debug)

	logger.Info("starting conector")

	ctx, cancel := context.WithCancel(context.Background())
	connector := connector.New(ctx, logger,
		cfg.Markets,
		cfg.Start,
		cfg.End,
		cfg.Rps,
		time.Duration(cfg.TaskQuantSeconds)*time.Second,
		cfg.PubSubProjectId,
		cfg.PubSubTopic,
	)

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

func populateMarkets(cfg *config.Config) {
	client := binance.NewFuturesClient("", "")
	exchangeInfo, err := client.NewExchangeInfoService().Do(context.Background())
	if err != nil {
		panic(err)
	}

	for _, s := range exchangeInfo.Symbols {
		cfg.Markets = append(cfg.Markets, s.Symbol)
	}
}
