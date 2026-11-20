#!/bin/bash
# usage: rundiff.sh script.py "<server flags>"  -> runs against go and rust, diffs normalized output
SP=${SCRATCH}/tools
R=/home/daniele/Documents/apps/cabby/target/release/cabby; G=/home/daniele/Documents/apps/cabby/target/reference-bin/anycable-go
script=$1; shift
cd $SP
for pair in "go:$G:19281" "rust:$R:19282"; do name=${pair%%:*}; rest=${pair#*:}; bin=${rest%%:*}; port=${rest##*:};
  $bin --port $port --http_broadcast_port $((port+100)) "$@" > $SP/$name.srv.log 2>&1 & S=$!
  sleep 1.2
  timeout 120 python3 $script $port $((port+100)) > $SP/$name.out 2>&1
  sleep 0.3; kill $S 2>/dev/null; wait $S 2>/dev/null
done
diff $SP/go.out $SP/rust.out > $SP/last.diff && echo "IDENTICAL ($(wc -l < $SP/go.out) lines)" || { echo "DIFF:"; cat $SP/last.diff | cut -c1-600; }
