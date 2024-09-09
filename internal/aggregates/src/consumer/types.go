package consumer

type Aggregate struct {
	AggTimestamp uint64 `ch:"agg_timestamp"`

	Exchange string `ch:"exchange"`
	Market   string `ch:"market"`
	Base     string `ch:"base"`
	Quot     string `ch:"quot"`

	PriceOpen                    *float64 `ch:"price_open"`
	PriceClose                   *float64 `ch:"price_close"`
	PriceHigh                    *float64 `ch:"price_high"`
	PriceLow                     *float64 `ch:"price_low"`
	VolumeBase                   *float64 `ch:"volume_base"`
	VolumeQuot                   *float64 `ch:"volume_quot"`
	VolumeBaseBuyTaker           *float64 `ch:"volume_base_buy_taker"`
	VolumeQuotBuyTaker           *float64 `ch:"volume_quot_buy_taker"`
	VolumeBaseSellTaker          *float64 `ch:"volume_base_sell_taker"`
	VolumeQuotSellTaker          *float64 `ch:"volume_quot_sell_taker"`
	OiOpen                       *float64 `ch:"oi_open"`
	TradesCount                  *uint64  `ch:"trades_count"`
	LiquidationsShortsCount      *uint64  `ch:"liquidations_shorts_count"`
	LiquidationsLongsCount       *uint64  `ch:"liquidations_longs_count"`
	LiquidationsShortsBaseVolume *float64 `ch:"liquidations_shorts_base_volume"`
	LiquidationsLongsBaseVolume  *float64 `ch:"liquidations_longs_base_volume"`
	LiquidationsShortsQuotVolume *float64 `ch:"liquidations_shorts_quot_volume"`
	LiquidationsLongsQuotVolume  *float64 `ch:"liquidations_longs_quot_volume"`
	FundingRate                  *float64 `ch:"funding_rate"`
	FundingPrice                 *float64 `ch:"funding_price"`
}
