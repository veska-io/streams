WITH filtered_dataset AS (
	SELECT
		symbol,
		base,
		quot,

		kline_timestamp,
		open_time,
		close_time,

		open,
		high,
		low,
		close,
		volume,

		trade_num,

		quot_asset_volume,
		taker_buy_base_asset_volume,
		taker_buy_quot_asset_volume,

		updated_timestamp

	FROM binance_futures_klines_1h
	WHERE
		kline_timestamp >= @fromTimestamp AND kline_timestamp <= @toTimestamp

	ORDER BY
		kline_timestamp ASC, symbol ASC, updated_timestamp DESC
)

SELECT
	LOWER(symbol) symbol,
	LOWER(any(base)) as base,
	LOWER(any(quot)) as quot,

	kline_timestamp,
	groupArray(open_time)[1] as open_time,
	groupArray(close_time)[1] as close_time,


	groupArray(open)[1] as open,
	groupArray(high)[1] as high,
	groupArray(low)[1] as low,
	groupArray(close)[1] as close,
	groupArray(volume)[1] as volume,


	groupArray(trade_num)[1] as trade_num,

	groupArray(quot_asset_volume)[1] as quot_asset_volume,
	groupArray(taker_buy_base_asset_volume)[1] as taker_buy_base_asset_volume,
	groupArray(taker_buy_quot_asset_volume)[1] as taker_buy_quot_asset_volume,

	groupArray(updated_timestamp)[1] as updated_timestamp

FROM filtered_dataset
GROUP BY symbol, kline_timestamp
ORDER BY kline_timestamp ASC, symbol ASC