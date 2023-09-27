CREATE TEMP FUNCTION starttime() AS (timestamp_sub(current_timestamp(), interval 45 hour));
CREATE TEMP FUNCTION endtime() AS (current_timestamp());


CREATE TABLE `maximal-furnace-783.rajat.stress_test_dataset`
OPTIONS (
  expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
) AS (
  SELECT 
    userId,
    ARRAY_AGG(feedType)[SAFE_OFFSET(0)] AS feedType,
    ARRAY_AGG(requestPayload)[SAFE_OFFSET(0)] AS payload,
    ARRAY_AGG(requestId)[SAFE_OFFSET(0)] AS requestId,
    ARRAY_AGG(sessionId)[SAFE_OFFSET(0)] AS sessionId,
    ARRAY_AGG(language)[SAFE_OFFSET(0)] AS language,
  FROM 
  (
    select userId , requestPayload , feedType  , requestId ,language ,   JSON_EXTRACT_SCALAR(feedOptions, '$.SessionId') as sessionId from `maximal-furnace-783.feed_relevance_service.feed_relevance_requests`
  where feedType in ("video-feed" ,"trending" , "video-suggestion" )
  and language in ('Hindi','Marathi','Kannada','Tamil','Telugu','Punjabi')
  and partitionTime BETWEEN starttime() AND endtime()
  and requestId != "" and requestId is not null
  and JSON_EXTRACT_SCALAR(feedOptions, '$.SessionId') != "" and JSON_EXTRACT_SCALAR(feedOptions, '$.SessionId') is not null 
  )
  GROUP BY userId
  ORDER by RAND()
)