package consumer

import (
	"context"
	"log/slog"
	"time"

	"github.com/adshao/go-binance/v2"
	common "github.com/adshao/go-binance/v2/common"
	"github.com/adshao/go-binance/v2/futures"
	restc "github.com/veska-io/streams-connectors/consumers/rest"
)

type OpenInterest struct {
	Oi    *futures.OpenInterest
	Ratio []*futures.LongShortRatio
}

type Consumer struct {
	restc.Consumer
}

func New(ctx context.Context, logger *slog.Logger,
	markets []string,
	start, end time.Time,
	rps uint8,
	taskQuant time.Duration,
) *Consumer {
	c := Consumer{}
	c.UpdateConfig(ctx, logger, markets, start, end, rps, taskQuant)
	c.ResponseChan = make(chan *restc.ResponseMessage)
	c.ApiCall = ApiCall

	return &c
}

func ApiCall(task restc.Task) (*restc.ResponseMessage, error) {
	restClient := binance.NewFuturesClient("", "")
	oi, err := restClient.NewGetOpenInterestService().Symbol(task.Market).Do(context.Background())

	if err != nil {
		e, _ := err.(*common.APIError)
		if e.Code != -4108 {
			return nil, e
		}
	}

	ratio, err := restClient.NewLongShortRatioService().Symbol(task.Market).Period("1h").Limit(1).Do(context.Background())
	if err != nil {
		e, _ := err.(*common.APIError)
		if e.Code != -4108 {
			return nil, e
		}
	}

	start_datetime := task.Start
	end_datetime := task.End

	msg := restc.ResponseMessage{
		Task:  task,
		Start: start_datetime,
		End:   end_datetime,
		Data:  &OpenInterest{Oi: oi, Ratio: ratio},
		Last:  true,
	}

	return &msg, nil
}
