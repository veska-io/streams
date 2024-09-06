package connector

import (
	"fmt"
	"strconv"
	"time"

	local_consumer "github.com/veska-io/streams-connectors/binance/futures/open-interest/events-generator/src/consumer"
	eeventspb "github.com/veska-io/streams-proto/gen/go/streams"
)

func ExtractEvents(funding local_consumer.OpenInterest) ([]*eeventspb.ExchangesEvent, error) {
	events := []*eeventspb.ExchangesEvent{}

	fundingEvent, err := ExtractFundingEvent(funding)
	if err != nil {
		return events, err
	} else {
		events = append(events, fundingEvent)
	}

	return events, nil
}

func ExtractFundingEvent(openInterest local_consumer.OpenInterest) (*eeventspb.ExchangesEvent, error) {
	oiOpen, err := strconv.ParseFloat(openInterest.OpenInterest, 64)
	if err != nil {
		return nil, fmt.Errorf("unable to parse openInterest: %w", err)
	}

	event := &eeventspb.ExchangesEvent{
		EventTimestamp: openInterest.OiTimestamp,

		Exchange: "binance",
		Market:   fmt.Sprintf("%s-%s", openInterest.Base, "usd"),
		Base:     openInterest.Base,
		Quot:     "usd",

		Event: &eeventspb.ExchangesEvent_Oi{
			Oi: &eeventspb.ExchangesEvent_OiEvent{
				OiOpen: oiOpen,
			},
		},

		ProcessedTimestamp: uint64(time.Now().UTC().UnixMilli()),
	}

	return event, nil
}
