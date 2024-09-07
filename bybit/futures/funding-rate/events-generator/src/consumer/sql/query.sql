WITH filtered_dataset AS (
	SELECT
		symbol,
		base,
		quot,

		funding_timestamp,

		rate,

		updated_timestamp

	FROM bybit_futures_funding_1h
	WHERE
		funding_timestamp >= @fromTimestamp AND funding_timestamp <= @toTimestamp

	ORDER BY
		funding_timestamp ASC, symbol ASC, updated_timestamp DESC
)

SELECT
	LOWER(symbol) symbol,
	LOWER(any(base)) as base,
	LOWER(any(quot)) as quot,

	funding_timestamp,

	groupArray(rate)[1] as rate,

	groupArray(updated_timestamp)[1] as updated_timestamp

FROM filtered_dataset
GROUP BY symbol, funding_timestamp
ORDER BY funding_timestamp ASC, symbol ASC