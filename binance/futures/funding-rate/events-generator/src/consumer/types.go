package consumer

type Funding struct {
	Symbol string `ch:"symbol"`
	Base   string `ch:"base"`
	Quot   string `ch:"quot"`

	FundingTimestamp uint64 `ch:"funding_timestamp"`

	Rate      string `ch:"rate"`
	MarkPrice string `ch:"mark_price"`

	UpdatedTimestamp string `ch:"updated_timestamp"`
}
