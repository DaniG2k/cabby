#!/bin/bash
port=$1; bport=$2
b() { curl -s -o /dev/null -w "post:%{http_code}\n" -X POST localhost:$bport/_broadcast -d "$1"; }
g() { echo "### $*"; curl -s -i -m 3 "$@" 2>&1 | tr -d '\r' | grep -vi "^date:" ; echo; echo "(curl rc=$?)"; }
b '{"stream":"s","data":"{\"n\":1}"}'; b '{"stream":"s","data":"{\"n\":2}"}'; b '{"stream":"s","data":"plain text"}'
g "localhost:$port/ds/s"
g "localhost:$port/ds/s?offset=-1"
g "localhost:$port/ds/s?offset=now"
g "localhost:$port/ds/s?offset=0"
g "localhost:$port/ds/s?offset=bogus"
g "localhost:$port/ds/s?offset=-1&live=long-poll"
g "localhost:$port/ds/s?offset=now&live=long-poll" -m 2
g "localhost:$port/ds/s?offset=-1&live=sse" -m 2
g "localhost:$port/ds/s?offset=-1&live=bogus"
g "localhost:$port/ds/unknown-stream?offset=-1"
g -X HEAD "localhost:$port/ds/s"
g -X POST "localhost:$port/ds/s" -d 'x'
g -X DELETE "localhost:$port/ds/s"
g -X OPTIONS "localhost:$port/ds/s" -H "Origin: http://x.test"
g "localhost:$port/ds/"
g "localhost:$port/ds"
g "localhost:$port/ds/a/b/c?offset=-1"
g "localhost:$port/ds/s?offset=-1" -H 'If-None-Match: "x"'
g "localhost:$port/health"
g "localhost:$port/nope"
g -X POST "localhost:$port/health"
g "localhost:$port/cable"
g "localhost:$port/events"
g "localhost:$port/events?channel=x"
g "localhost:$port/events?stream=s" -m 2
g "localhost:$port/events?identifier=%7B%22channel%22%3A%22%24pubsub%22%2C%22stream_name%22%3A%22s%22%7D" -m 2
g -X POST "localhost:$port/events" -m 2 -d '{"command":"subscribe","identifier":"{\"channel\":\"$pubsub\",\"stream_name\":\"s\"}"}'
g -X OPTIONS "localhost:$port/events" -H "Origin: http://x.test"
