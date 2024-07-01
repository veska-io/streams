package consumer

import (
	"context"
	"log/slog"

	"github.com/cloudevents/sdk-go/v2/event"
	binancepb "github.com/veska-io/streams-proto/gen/go/streams/binance/futures"
	"google.golang.org/protobuf/proto"
)

type Consumer struct {
	event event.Event

	ctx    context.Context
	logger *slog.Logger

	ResponseChannel chan *binancepb.Kline
}

func New(ctx context.Context, logger *slog.Logger, e event.Event) *Consumer {
	return &Consumer{
		event: e,

		ctx:    ctx,
		logger: logger,

		ResponseChannel: make(chan *binancepb.Kline),
	}
}

func (c *Consumer) Run() {
	kline, err := c.GetKline()

	if err != nil {
		c.logger.Error("unable to parse the kline", slog.String("error", err.Error()))
	} else {
		c.ResponseChannel <- kline
	}

	close(c.ResponseChannel)
}

func (c *Consumer) GetKline() (*binancepb.Kline, error) {
	var msg MessagePublishedData

	if err := c.event.DataAs(&msg); err != nil {
		return nil, err
	}

	kline := &binancepb.Kline{}
	if err := proto.Unmarshal(msg.Message.Data, kline); err != nil {
		return nil, err
	}

	return kline, nil
}
