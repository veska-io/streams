package producer

import (
	"context"
	"fmt"
	"log/slog"

	pub_sub "github.com/veska-io/streams-connectors/producers/pub-sub"
	eeventspb "github.com/veska-io/streams-proto/gen/go/streams"
	"google.golang.org/protobuf/proto"
)

type Producer struct {
	DataStream   chan *eeventspb.ExchangesEvent
	StatusStream chan any

	pubsubProducer *pub_sub.Producer

	ctx    context.Context
	logger *slog.Logger
}

func New(
	ctx context.Context,
	logger *slog.Logger,

	pubsubProjectId, pubsubTopic string,
) (*Producer, error) {
	p, err := pub_sub.New(ctx, logger,
		pubsubProjectId, pubsubTopic,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub producer: %w", err)
	}

	return &Producer{
		DataStream:     make(chan *eeventspb.ExchangesEvent),
		StatusStream:   make(chan any),
		pubsubProducer: p,

		ctx:    ctx,
		logger: logger,
	}, nil
}

func (p *Producer) Run() {
	go p.pubsubProducer.Run()

	go func() {
		for msg := range p.pubsubProducer.StatusStream {
			if msg.Error != nil {
				continue
			}

			p.StatusStream <- msg
		}

		close(p.StatusStream)
	}()

	for event := range p.DataStream {
		msg, err := proto.Marshal(event)
		if err != nil {
			p.logger.Error("failed to marshal event", slog.String("err", err.Error()))
		}

		p.pubsubProducer.DataStream <- pub_sub.Message{
			Data: msg,
		}
	}

	close(p.pubsubProducer.DataStream)
}

func (p *Producer) GetDataStream() chan<- *eeventspb.ExchangesEvent {
	return p.DataStream
}

func (p *Producer) GetStatusStream() <-chan any {
	return p.StatusStream
}
