create view dns as
select
	_timestamp,
	ts,
	day,
	_hostname,
	_source,

	uid,
	"id.orig_h", "id.orig_p",
	"id.resp_h", "id.resp_p",
	`string.values`[indexOf(`string.names`, 'proto')] AS proto,
	`number.values`[indexOf(`number.names`, 'trans_id')] AS trans_id,
	`number.values`[indexOf(`number.names`, 'rtt')] AS rtt,
	`string.values`[indexOf(`string.names`, 'query')] AS query,
	`number.values`[indexOf(`number.names`, 'qclass')] AS qclass,
	`string.values`[indexOf(`string.names`, 'qclass_name')] AS qclass_name,
	`number.values`[indexOf(`number.names`, 'qtype')] AS qtype,
	`string.values`[indexOf(`string.names`, 'qtype_name')] AS qtype_name,
	`number.values`[indexOf(`number.names`, 'rcode')] AS rcode,
	`string.values`[indexOf(`string.names`, 'rcode_name')] AS rcode_name,
	`bool.values`[indexOf(`bool.names`, 'AA')] AS AA,
	`bool.values`[indexOf(`bool.names`, 'TC')] AS TC,
	`bool.values`[indexOf(`bool.names`, 'RD')] AS RD,
	`bool.values`[indexOf(`bool.names`, 'RA')] AS RA,
	`number.values`[indexOf(`number.names`, 'Z')] AS Z,
	`array.values`[indexOf(`array.names`, 'answers')] AS answers,
	`array.values`[indexOf(`array.names`, 'TTLs')] AS TTLs,
	`bool.values`[indexOf(`bool.names`, 'rejected')] AS rejected
FROM logs
WHERE _path='dns'
;
