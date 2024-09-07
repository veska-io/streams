WITH filtered_dataset AS (
	SELECT
		symbol,
		base,
		quot,

		oi_timestamp,

		open_interest,
		buy_ratio,
		sell_ratio,

		updated_timestamp

	FROM bybit_futures_open_interest_1h
	WHERE
		oi_timestamp >= @fromTimestamp AND oi_timestamp <= @toTimestamp

	ORDER BY
		oi_timestamp ASC, symbol ASC, updated_timestamp DESC
)

SELECT
	LOWER(symbol) symbol,
	LOWER(any(base)) as base,
	LOWER(any(quot)) as quot,

	oi_timestamp,

	groupArray(open_interest)[1] as open_interest,
	groupArray(buy_ratio)[1] as buy_ratio,
	groupArray(sell_ratio)[1] as sell_ratio,

	groupArray(updated_timestamp)[1] as updated_timestamp

FROM filtered_dataset
GROUP BY symbol, oi_timestamp
ORDER BY oi_timestamp ASC, symbol ASC