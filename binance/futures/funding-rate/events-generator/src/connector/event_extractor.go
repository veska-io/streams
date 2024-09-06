package connector

import (
	"fmt"
	"strconv"
	"time"

	local_consumer "github.com/veska-io/streams-connectors/binance/futures/funding-rate/events-generator/src/consumer"
	eeventspb "github.com/veska-io/streams-proto/gen/go/streams"
)

func ExtractEvents(funding local_consumer.Funding) ([]*eeventspb.ExchangesEvent, error) {
	events := []*eeventspb.ExchangesEvent{}

	fundingEvent, err := ExtractFundingEvent(funding)
	if err != nil {
		return events, err
	} else {
		events = append(events, fundingEvent)
	}

	return events, nil
}

func ExtractFundingEvent(funding local_consumer.Funding) (*eeventspb.ExchangesEvent, error) {
	fundingRate, err := strconv.ParseFloat(funding.Rate, 64)
	if err != nil {
		return nil, fmt.Errorf("unable to parse funding rate: %w", err)
	}

	fundingPrice, err := strconv.ParseFloat(funding.MarkPrice, 64)
	if err != nil {
		return nil, fmt.Errorf("unable to parse price high: %w", err)
	}

	event := &eeventspb.ExchangesEvent{
		EventTimestamp: funding.FundingTimestamp,

		Exchange: "binance",
		Market:   fmt.Sprintf("%s-%s", funding.Base, "usd"),
		Base:     funding.Base,
		Quot:     "usd",

		Event: &eeventspb.ExchangesEvent_FundingRate{
			FundingRate: &eeventspb.ExchangesEvent_FundingRateEvent{
				FundingRate:  fundingRate,
				FundingPrice: fundingPrice,
			},
		},

		ProcessedTimestamp: uint64(time.Now().UTC().UnixMilli()),
	}

	return event, nil
}
