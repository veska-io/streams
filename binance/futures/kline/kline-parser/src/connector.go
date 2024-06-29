package connector

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
	binancepb "github.com/veska-io/streams-proto/gen/go/streams/binance"
	eeventspb "github.com/veska-io/streams-proto/gen/go/streams/main"
	"google.golang.org/protobuf/proto"
)

const (
	EXCHANGE = "binance"
)

func init() {
	functions.CloudEvent("binance_kline", processKline)
}

// MessagePublishedData contains the full Pub/Sub message
// See the documentation for more details:
// https://cloud.google.com/eventarc/docs/cloudevents#pubsub
type MessagePublishedData struct {
	Message PubSubMessage
}

// PubSubMessage is the payload of a Pub/Sub event.
// See the documentation for more details:
// https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
type PubSubMessage struct {
	Data []byte `json:"data"`
}

// helloPubSub consumes a CloudEvent message and extracts the Pub/Sub message.
func processKline(ctx context.Context, e event.Event) error {
	var msg MessagePublishedData
	if err := e.DataAs(&msg); err != nil {
		return fmt.Errorf("event.DataAs: %w", err)
	}

	kline := &binancepb.Kline{}
	if err := proto.Unmarshal(msg.Message.Data, kline); err != nil {
		return fmt.Errorf("proto.Unmarshal: %w", err)
	}

	baseAsset := strings.Split(strings.ToLower(kline.GetSymbol()), `-`)[0]
	quoteAsset := "usd"

	priceEvent := eeventspb.Event{
		Timestamp: uint64(time.UnixMilli(kline.GetOpenTime()).Truncate(time.Hour).UnixMilli()),
		Event:     "price",

		Exchange: EXCHANGE,
		Market:   baseAsset + "-" + quoteAsset,
		Base:     baseAsset,
		Quot:     quoteAsset,

		
	}

	log.Printf(kline.Open)

	return nil
}

func truncateToHour(dateTime time.Time) {
	dateTime.Truncate(time.Hour)
}
