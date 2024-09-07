package consumer

type Funding struct {
	Symbol string `ch:"symbol"`
	Base   string `ch:"base"`
	Quot   string `ch:"quot"`

	FundingTimestamp uint64 `ch:"funding_timestamp"`

	Rate string `ch:"rate"`

	UpdatedTimestamp string `ch:"updated_timestamp"`
}
