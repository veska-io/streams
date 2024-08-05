package consumer

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/hirokisan/bybit/v2"
	restc "github.com/veska-io/streams-connectors/consumers/rest"
)

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
	restClient := bybit.NewClient()

	startTime := task.Start.UnixMilli()
	endTime := task.End.UnixMilli()
	limit := 200

	fRate, err := restClient.V5().Market().GetFundingRateHistory(bybit.V5GetFundingRateHistoryParam{
		Category:  "linear",
		Symbol:    bybit.SymbolV5(task.Market),
		StartTime: &startTime,
		EndTime:   &endTime,

		Limit: &limit,
	})

	if err != nil {
		return nil, err
	}

	start_datetime := task.Start
	end_datetime := task.End

	if len(fRate.Result.List) > 0 {
		endTimestamp, err := strconv.ParseInt(fRate.Result.List[0].FundingRateTimestamp, 10, 64)
		if err != nil {
			return nil, err
		}

		startTimestamp, err := strconv.ParseInt(fRate.Result.List[len(fRate.Result.List)-1].FundingRateTimestamp, 10, 64)
		if err != nil {
			return nil, err
		}

		start_datetime = time.UnixMilli(startTimestamp).UTC().Truncate(time.Hour)
		end_datetime = time.UnixMilli(endTimestamp).UTC().Truncate(time.Hour)
	}

	msg := restc.ResponseMessage{
		Task:  task,
		Start: start_datetime,
		End:   end_datetime,
		Data:  fRate,
		Last:  len(fRate.Result.List) < 200,
	}

	return &msg, nil
}
