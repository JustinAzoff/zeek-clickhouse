CREATE TABLE if not exists logs
(
	_timestamp	UInt64,
	_path		String,
	_hostname	String,

	//raw log event
	_source		String,

	//type specific fields names and field values
	"string.names"	Array(LowCardinality(String)),
	"string.values"	Array(String),
	"number.names"	Array(LowCardinality(String)),
	"number.values"	Array(Float64),
	"bool.names"	Array(LowCardinality(String)),
	"bool.values"	Array(UInt8),

	"array.names"	Array(LowCardinality(String)),
	"array.values"	Array(Array(String)),

	//Materialized fields
	ts		DateTime DEFAULT FROM_UNIXTIME(toUInt64(_timestamp/1000)),
	day		Date DEFAULT toDate(FROM_UNIXTIME(toUInt64(_timestamp/1000))),
	uid		String DEFAULT "string.values"[indexOf("string.names", 'uid')],
	"id.orig_h"	String DEFAULT "string.values"[indexOf("string.names", 'id.orig_h')],
	"id.orig_p"	String DEFAULT "number.values"[indexOf("number.names", 'id.orig_p')],
	"id.resp_h"	String DEFAULT "string.values"[indexOf("string.names", 'id.resp_h')],
	"id.resp_p"	String DEFAULT "number.values"[indexOf("number.names", 'id.resp_p')]
)
ENGINE = MergeTree()
ORDER BY (_timestamp, cityHash64(uid))
SAMPLE BY (cityHash64(uid))
PARTITION BY (toYYYYMM(FROM_UNIXTIME(toUInt64(_timestamp/1000))), _path);
