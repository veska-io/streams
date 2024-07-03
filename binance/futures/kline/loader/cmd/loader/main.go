package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/veska-io/streams-connectors/binance/futures/kline/loader/src/logger"

	loader "github.com/veska-io/streams-connectors/binance/futures/kline/loader/src"
	"github.com/veska-io/streams-connectors/binance/futures/kline/loader/src/config"

	"github.com/adshao/go-binance/v2"
)

func main() {
	cfg := config.MustNew()
	if len(cfg.Markets) == 0 {
		populateMarkets(cfg)
	}
	logger := logger.New(cfg.Debug)

	logger.Info("starting conector")

	ctx, cancel := context.WithCancel(context.Background())
	connector := loader.New(ctx, logger, cfg)

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
