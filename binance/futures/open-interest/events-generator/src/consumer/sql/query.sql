WITH filtered_dataset AS (
	SELECT
		symbol,
		base,
		quot,

		oi_timestamp,

		open_interest,
		long_short_ratio,
		long_account,
		short_account,

		updated_timestamp

	FROM binance_futures_open_interest_1h
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
	groupArray(long_short_ratio)[1] as long_short_ratio,
	groupArray(long_account)[1] as long_account,
	groupArray(short_account)[1] as short_account,

	groupArray(updated_timestamp)[1] as updated_timestamp

FROM filtered_dataset
GROUP BY symbol, oi_timestamp
ORDER BY oi_timestamp ASC, symbol ASC