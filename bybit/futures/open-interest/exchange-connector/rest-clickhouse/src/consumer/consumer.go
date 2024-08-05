package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"encoding/json"
	"io"
	"net/http"

	"github.com/hirokisan/bybit/v2"
	restc "github.com/veska-io/streams-connectors/consumers/rest"
)

type LongShortRatio struct {
	Symbol    string `json:"symbol"`
	BuyRatio  string `json:"buyRatio"`
	SellRatio string `json:"sellRatio"`
	Timestamp string `json:"timestamp"`
}

type LongShortRatioResponse struct {
	RetCode    int                    `json:"retCode"`
	RetMsg     string                 `json:"retMsg"`
	Result     LongShortRatioResult   `json:"result"`
	RetExtInfo map[string]interface{} `json:"retExtInfo"`
	Time       uint64                 `json:"time"`
}

type LongShortRatioResult struct {
	List []LongShortRatio `json:"list"`
}

type OpenInterest struct {
	Oi    *bybit.V5GetOpenInterestItem
	Ratio *LongShortRatio
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
	restClient := bybit.NewClient()

	limit := 1

	oiResult, err := restClient.V5().Market().GetOpenInterest(bybit.V5GetOpenInterestParam{
		Category:     "linear",
		Symbol:       bybit.SymbolV5(task.Market),
		IntervalTime: "1h",
		Limit:        &limit,
	})

	if err != nil {
		return nil, err
	}

	start_datetime := task.Start
	end_datetime := task.End

	msg := restc.ResponseMessage{
		Task:  task,
		Start: start_datetime,
		End:   end_datetime,
		Data:  &OpenInterest{Oi: nil, Ratio: nil},
		Last:  true,
	}

	if len(oiResult.Result.List) > 0 {
		endTimestamp, err := strconv.ParseInt(oiResult.Result.List[0].Timestamp, 10, 64)
		if err != nil {
			return nil, err
		}

		startTimestamp, err := strconv.ParseInt(oiResult.Result.List[len(oiResult.Result.List)-1].Timestamp, 10, 64)
		if err != nil {
			return nil, err
		}

		start_datetime = time.UnixMilli(startTimestamp).UTC().Truncate(time.Hour)
		end_datetime = time.UnixMilli(endTimestamp).UTC().Truncate(time.Hour)

		ratio, err := GetLongShortRatio(task.Market)
		if err != nil {
			fmt.Printf("error while fetching long short ratio: %s", err.Error())
			ratio = &LongShortRatio{Symbol: task.Market, BuyRatio: "", SellRatio: "", Timestamp: ""}
		}

		msg.Data = &OpenInterest{Oi: &oiResult.Result.List[0], Ratio: ratio}
		msg.Start = start_datetime
		msg.End = end_datetime
	}

	return &msg, nil
}

func GetLongShortRatio(symbol string) (*LongShortRatio, error) {
	var ratio LongShortRatioResponse

	requestUrl := "https://api.bybit.com/v5/market/account-ratio?symbol=" +
		symbol + "&category=linear&period=1h&limit=1"

	resp, err := http.Get(requestUrl)
	if err != nil {
		return nil, fmt.Errorf("error while fetching data from external API: %w", err)
	}

	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("error while reading response body: %w", err)
	}

	err = json.Unmarshal(body, &ratio)
	if err != nil {
		return nil, err
	}

	if len(ratio.Result.List) == 0 {
		return nil, fmt.Errorf("empty response from external API")
	}

	return &ratio.Result.List[0], nil
}
