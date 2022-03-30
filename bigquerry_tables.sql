--Pageview
CREATE OR REPLACE VIEW ga_data.pageview AS
SELECT 
	date, pagePath, source, channel_grouping, device_category, country, city,
	COUNT(*) AS pageviews, COUNT(DISTINCT session_id) AS unique_pageviews
FROM ( 
	SELECT 
		PARSE_DATE("%Y%m%d", date) AS date,
		hits.page.pagepath AS pagePath, 
		trafficsource.source AS source, 
		channelgrouping as channel_grouping, 
		device.deviceCategory as device_category, 
		geonetwork.country as country, 
		geonetwork.city as city, 
		CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)) AS session_id 
	FROM `bigquery-public-data.google_analytics_sample.ga_sessions*` AS GA, UNNEST(GA.hits) AS hits 
	WHERE hits.type = 'PAGE' 
)
GROUP BY date, pagePath, source, channel_grouping, device_category, country, city 
;


--Time On Page
CREATE OR REPLACE VIEW ga_data.time_on_page AS
SELECT 
	date, pagePath, source, channel_grouping, device_category, country, city,
	SUM(time_on_page_combined) as total_time_on_page
FROM (
	SELECT *,
		CASE WHEN isExit IS TRUE THEN last_interaction_second - hit_time_second
			ELSE next_pageview_second - hit_time_second END as time_on_page_combined
	FROM (
		SELECT *, LEAD(hit_time_second) OVER (PARTITION BY fullVisitorId, visitStartTime ORDER BY hit_time_second) AS next_pageview_second
		FROM ( 
			SELECT
				PARSE_DATE("%Y%m%d", date) AS date,
				fullVisitorId, 
				visitStartTime,
				hits.page.pagepath AS pagePath, 
				trafficsource.source AS source, 
				channelgrouping AS channel_grouping, 
				device.deviceCategory AS device_category, 
				geonetwork.country AS country, 
				geonetwork.city AS city, 
				hits.type,
				hits.isExit,
				hits.time/1000 AS hit_time_second,
				MAX(hits.time/1000) OVER (PARTITION BY fullVisitorId, visitStartTime) as last_interaction_second
			FROM `bigquery-public-data.google_analytics_sample.ga_sessions*` AS GA, UNNEST(GA.hits) AS hits 
			WHERE hits.isInteraction is TRUE
		)
		WHERE type = 'PAGE'
	)
)
GROUP BY date, pagePath, source, channel_grouping, device_category, country, city 
;


--Bounce
CREATE OR REPLACE VIEW ga_data.bounce AS
SELECT
	date, pagePath, source, channel_grouping, device_category, country, city,
	SUM(page_bounces) AS total_bounces
FROM (
	SELECT
		*,
		CASE WHEN hitNumber = first_interaction THEN bounces ELSE 0 END AS page_bounces
	FROM (
		SELECT
			PARSE_DATE("%Y%m%d", date) AS date,
			fullVisitorId, 
			visitStartTime,
			hits.page.pagepath AS pagePath, 
			trafficsource.source AS source, 
			channelgrouping AS channel_grouping, 
			device.deviceCategory AS device_category, 
			geonetwork.country AS country, 
			geonetwork.city AS city, 
			totals.bounces,
			hits.hitNumber,
			MIN(hits.hitNumber) OVER (PARTITION BY fullVisitorId, visitStartTime) AS first_interaction
		FROM `bigquery-public-data.google_analytics_sample.ga_sessions*` AS GA, UNNEST(GA.hits) AS hits
		WHERE hits.isInteraction IS TRUE
	)
)
GROUP BY date, pagePath, source, channel_grouping, device_category, country, city
;

--Session
CREATE OR REPLACE VIEW ga_data.session AS
SELECT
	date, pagePath, source, channel_grouping, device_category, country, city,
	SUM(sessions) AS total_sessions
FROM (
	SELECT
		*,
		CASE WHEN hitNumber = first_interaction THEN visits ELSE 0 END AS sessions
	FROM (
		SELECT
			PARSE_DATE("%Y%m%d", date) AS date,
			fullVisitorId, 
			visitStartTime,
			hits.page.pagepath AS pagePath, 
			trafficsource.source AS source, 
			channelgrouping AS channel_grouping, 
			device.deviceCategory AS device_category, 
			geonetwork.country AS country, 
			geonetwork.city AS city, 
			totals.visits, 
			hits.hitNumber,
			MIN(hits.hitNumber) OVER (PARTITION BY fullVisitorId, visitStartTime) AS first_interaction
		FROM `bigquery-public-data.google_analytics_sample.ga_sessions*` AS GA, UNNEST(GA.hits) AS hits
		WHERE hits.isInteraction IS TRUE
	)
)
GROUP BY date, pagePath, source, channel_grouping, device_category, country, city
;

-- Full Table
SELECT *
FROM `gothic-isotope-342421.ga_data.pageview`
FULL JOIN `gothic-isotope-342421.ga_data.time_on_page` USING (date, pagePath, source, channel_grouping, device_category, country, city)
FULL JOIN `gothic-isotope-342421.ga_data.session` USING (date, pagePath, source, channel_grouping, device_category, country, city)
FULL JOIN `gothic-isotope-342421.ga_data.bounce` USING (date, pagePath, source, channel_grouping, device_category, country, city)



