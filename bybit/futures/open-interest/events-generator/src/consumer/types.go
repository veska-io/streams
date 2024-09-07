package consumer

type OpenInterest struct {
	Symbol      string `ch:"symbol"`
	Base        string `ch:"base"`
	Quot        string `ch:"quot"`
	OiTimestamp uint64 `ch:"oi_timestamp"`

	OpenInterest string `ch:"open_interest"`
	BuyRatio     string `ch:"buy_ratio"`
	SellRatio    string `ch:"sell_ratio"`

	UpdatedTimestamp uint64 `ch:"updated_timestamp"`
}
