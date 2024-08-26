package consumer

type Kline struct {
	Symbol                  string `ch:"symbol"`
	Base                    string `ch:"base"`
	Quot                    string `ch:"quot"`
	KlineTimestamp          uint64 `ch:"kline_timestamp"`
	OpenTime                uint64 `ch:"open_time"`
	CloseTime               uint64 `ch:"close_time"`
	Open                    string `ch:"open"`
	High                    string `ch:"high"`
	Low                     string `ch:"low"`
	Close                   string `ch:"close"`
	Volume                  string `ch:"volume"`
	TradeNum                uint64 `ch:"trade_num"`
	QuotAssetVolume         string `ch:"quot_asset_volume"`
	TakerBuyBaseAssetVolume string `ch:"taker_buy_base_asset_volume"`
	TakerBuyQuotAssetVolume string `ch:"taker_buy_quot_asset_volume"`
	UpdatedTimestamp        string `ch:"updated_timestamp"`
}
