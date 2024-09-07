WITH filtered_dataset AS (
	SELECT
		symbol,
		base,
		quot,

		kline_timestamp,
		start_time,

		open,
		high,
		low,
		close,

		volume,
		turnover,

		updated_timestamp

	FROM bybit_futures_klines_1h
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
	groupArray(start_time)[1] as start_time,


	groupArray(open)[1] as open,
	groupArray(high)[1] as high,
	groupArray(low)[1] as low,
	groupArray(close)[1] as close,

	groupArray(volume)[1] as volume,
	groupArray(turnover)[1] as turnover,

	groupArray(updated_timestamp)[1] as updated_timestamp

FROM filtered_dataset
GROUP BY symbol, kline_timestamp
ORDER BY kline_timestamp ASC, symbol ASC