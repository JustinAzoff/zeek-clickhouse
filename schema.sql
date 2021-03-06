CREATE TABLE logs
(
	_timestamp	UInt64,
	_path		String,
	hostname	String,

	//raw log event
	_source		String,

	//type specific fields names and field values
	"string.names"	Array(LowCardinality(String)),
	"string.values"	Array(String),
	"number.names"	Array(LowCardinality(String)),
	"number.values"	Array(Float64),
	"bool.names"	Array(LowCardinality(String)),
	"bool.values"	Array(UInt8),

	//Materialized fields
	uid		String DEFAULT "string.values"[indexOf("string.names", 'uid')]
	//id.orig_h	String,
	//id.orig_p	UInt16,
	//id.resp_h	String,
	//id.resp_p	UInt16,
)
ENGINE = MergeTree()
ORDER BY (_timestamp, cityHash64(uid))
SAMPLE BY (cityHash64(uid))
PARTITION BY (toYYYYMM(FROM_UNIXTIME(_timestamp)), _path);
