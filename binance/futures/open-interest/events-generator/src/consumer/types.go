package consumer

type OpenInterest struct {
	Symbol           string `ch:"symbol"`
	Base             string `ch:"base"`
	Quot             string `ch:"quot"`
	OiTimestamp      uint64 `ch:"oi_timestamp"`
	OpenInterest     string `ch:"open_interest"`
	LongShortRatio   string `ch:"long_short_ratio"`
	LongAccount      string `ch:"long_account"`
	ShortAccount     string `ch:"short_account"`
	UpdatedTimestamp uint64 `ch:"updated_timestamp"`
}
