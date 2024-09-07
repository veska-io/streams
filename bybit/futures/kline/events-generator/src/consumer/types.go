package consumer

type Kline struct {
	Symbol         string `ch:"symbol"`
	Base           string `ch:"base"`
	Quot           string `ch:"quot"`
	KlineTimestamp uint64 `ch:"kline_timestamp"`
	StartTime      string `ch:"start_time"`
	Open           string `ch:"open"`
	High           string `ch:"high"`
	Low            string `ch:"low"`
	Close          string `ch:"close"`

	Volume   string `ch:"volume"`
	Turnover string `ch:"turnover"`

	UpdatedTimestamp string `ch:"updated_timestamp"`
}
