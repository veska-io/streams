package connector

import (
	"context"
	"log/slog"
	"time"

	"github.com/adshao/go-binance/v2/futures"
	"github.com/veska-io/streams-connectors/binance/futures/open-interest/exchange-connector/rest-pubsub/src/consumer"
	pub_sub "github.com/veska-io/streams-connectors/producers/pub-sub"
	binancepb "github.com/veska-io/streams-proto/gen/go/streams/binance/futures"
	"google.golang.org/protobuf/proto"
)

type Connector struct {
	consumer *consumer.Consumer
	producer *pub_sub.Producer

	ctx    context.Context
	logger *slog.Logger
}

func New(ctx context.Context, logger *slog.Logger,
	markets []string, start, end time.Time, rps uint8, taskQuant time.Duration,
	pubsubProjectId, pubsubTopic string,
) *Connector {
	c := consumer.New(ctx, logger,
		markets,
		start,
		end,
		rps,
		taskQuant,
	)

	p, _ := pub_sub.New(ctx, logger,
		pubsubProjectId, pubsubTopic,
	)

	return &Connector{
		consumer: c,
		producer: p,

		ctx:    ctx,
		logger: logger,
	}
}

func (c *Connector) Run() {
	go c.consumer.Run()
	go c.producer.Run()

	go func() {
		for msg := range c.producer.StatusStream {
			if msg.Error != nil {
				c.logger.Error("producer status", slog.String("error", msg.Error.Error()))
				continue
			}
		}
	}()

	statusCounter := 0

	for response := range c.consumer.ResponseChan {
		if statusCounter == 0 {
			c.logger.Info("precent completed", slog.Uint64("status", uint64(c.consumer.Status())))
		}

		oiResponse, ok := response.Data.(*futures.OpenInterest)
		if !ok {
			c.logger.Error("failed to cast response data to OpenInterest")
			continue
		}

		if oiResponse == nil {
			c.logger.Warn("Empty OpenInterest")
			continue
		}

		open_interest := &binancepb.OpenInterest{
			Symbol: response.Task.Market,
			Base:   response.Task.Market[:len(response.Task.Market)-4],
			Quot:   response.Task.Market[len(response.Task.Market)-4:],

			OiTimestamp:  uint64(oiResponse.Time),
			OpenInterest: oiResponse.OpenInterest,
		}

		msg, err := proto.Marshal(open_interest)
		if err != nil {
			c.logger.Error("failed to marshal open_interest", slog.String("err", err.Error()))
		}

		c.producer.DataStream <- pub_sub.Message{
			Data: msg,
		}

		statusCounter += 1
		statusCounter = statusCounter % 10
	}

	close(c.producer.DataStream)
}
