#!/bin/sh

echo example 1 using arrow-cat
./hosts2arrow-ipc-stream |
	arrow-cat |
	tail -3

echo
echo example 2 using sql
./hosts2arrow-ipc-stream |
	rs-ipc-stream2df \
	--max-rows 1024 \
	--tabname 'hosts' \
	--sql "
		SELECT
			*
		FROM hosts
		ORDER BY names
	" |
	rs-arrow-ipc-stream-cat
