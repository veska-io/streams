WITH filtered_dataset AS (
	SELECT
		event,
		event_timestamp,
		exchange,
		market,
		base,
		quot,
		price_open,
		price_close,
		price_high,
		price_low,
		volume_base,
		volume_quot,
		volume_base_buy_taker,
		volume_quot_buy_taker,
		volume_base_sell_taker,
		volume_quot_sell_taker,
		oi_open,
		trades_count,
		liquidations_shorts_count,
		liquidations_longs_count,
		liquidations_shorts_base_volume,
		liquidations_longs_base_volume,
		liquidations_shorts_quot_volume,
		liquidations_longs_quot_volume,
		funding_rate,
		funding_price,
		updated_timestamp

	FROM futures_exchanges_events_1h
	WHERE
		event_timestamp >= @fromTimestamp AND event_timestamp <= @toTimestamp

	ORDER BY
		event_timestamp ASC, market ASC, updated_timestamp DESC
),

clean_dataset as (
	SELECT
		event,
		event_timestamp,
		exchange,
		market,

		any(base) as base,
		any(quot) as quot,

		IF(notEmpty(groupArray(price_open)), groupArray(price_open)[1], NULL) as price_open,
		IF(notEmpty(groupArray(price_close)), groupArray(price_close)[1], NULL) as price_close,
		IF(notEmpty(groupArray(price_high)), groupArray(price_high)[1], NULL) as price_high,
		IF(notEmpty(groupArray(price_low)), groupArray(price_low)[1], NULL) as price_low,
		IF(notEmpty(groupArray(volume_base)), groupArray(volume_base)[1], NULL) as volume_base,
		IF(notEmpty(groupArray(volume_quot)), groupArray(volume_quot)[1], NULL) as volume_quot,
		IF(notEmpty(groupArray(volume_base_buy_taker)), groupArray(volume_base_buy_taker)[1], NULL) as volume_base_buy_taker,
		IF(notEmpty(groupArray(volume_quot_buy_taker)), groupArray(volume_quot_buy_taker)[1], NULL) as volume_quot_buy_taker,
		IF(notEmpty(groupArray(volume_base_sell_taker)), groupArray(volume_base_sell_taker)[1], NULL) as volume_base_sell_taker,
		IF(notEmpty(groupArray(volume_quot_sell_taker)), groupArray(volume_quot_sell_taker)[1], NULL) as volume_quot_sell_taker,
		IF(notEmpty(groupArray(oi_open)), groupArray(oi_open)[1], NULL) as oi_open,
		IF(notEmpty(groupArray(trades_count)), groupArray(trades_count)[1], NULL) as trades_count,
		IF(notEmpty(groupArray(liquidations_shorts_count)), groupArray(liquidations_shorts_count)[1], NULL) as liquidations_shorts_count,
		IF(notEmpty(groupArray(liquidations_longs_count)), groupArray(liquidations_longs_count)[1], NULL) as liquidations_longs_count,
		IF(notEmpty(groupArray(liquidations_shorts_base_volume)), groupArray(liquidations_shorts_base_volume)[1], NULL) as liquidations_shorts_base_volume,
		IF(notEmpty(groupArray(liquidations_longs_base_volume)), groupArray(liquidations_longs_base_volume)[1], NULL) as liquidations_longs_base_volume,
		IF(notEmpty(groupArray(liquidations_shorts_quot_volume)), groupArray(liquidations_shorts_quot_volume)[1], NULL) as liquidations_shorts_quot_volume,
		IF(notEmpty(groupArray(liquidations_longs_quot_volume)), groupArray(liquidations_longs_quot_volume)[1], NULL) as liquidations_longs_quot_volume,
		IF(notEmpty(groupArray(funding_rate)), groupArray(funding_rate)[1], NULL) as funding_rate,
		IF(notEmpty(groupArray(funding_price)), groupArray(funding_price)[1], NULL) as funding_price,
		IF(notEmpty(groupArray(updated_timestamp)), groupArray(updated_timestamp)[1], NULL) as updated_timestamp 

	FROM filtered_dataset
	GROUP BY exchange, market, event, event_timestamp 
	ORDER BY event_timestamp ASC
)

SELECT
	event_timestamp as agg_timestamp,
	exchange,
	market,
	any(base) as base,
	any(quot) as quot,
	any(price_open) as price_open,
	any(price_close) as price_close,
	any(price_high) as price_high,
	any(price_low) as price_low,
	any(volume_base) as volume_base,
	any(volume_quot) as volume_quot,
	any(volume_base_buy_taker) as volume_base_buy_taker,
	any(volume_quot_buy_taker) as volume_quot_buy_taker,
	any(volume_base_sell_taker) as volume_base_sell_taker,
	any(volume_quot_sell_taker) as volume_quot_sell_taker,
	any(oi_open) as oi_open,
	any(trades_count) as trades_count,
	any(liquidations_shorts_count) as liquidations_shorts_count,
	any(liquidations_longs_count) as liquidations_longs_count,
	any(liquidations_shorts_base_volume) as liquidations_shorts_base_volume,
	any(liquidations_longs_base_volume) as liquidations_longs_base_volume,
	any(liquidations_shorts_quot_volume) as liquidations_shorts_quot_volume,
	any(liquidations_longs_quot_volume) as liquidations_longs_quot_volume,
	any(funding_rate) as funding_rate,
	any(funding_price) as funding_price
FROM
	clean_dataset
GROUP BY
	event_timestamp, exchange, market