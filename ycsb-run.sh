#!/bin/bash

# Simple YCSB execution script

cd "$(dirname "$0")/distalg-project2-base/client" || exit 1

THREADS=${1:-4}
PAYLOAD=${2:-1024}
SERVERS=${3:-"bulbasaur-3:35000,oddish-1:35000,charmander-5:35000"}
READ_PROP=${4:-0.5}
WRITE_PROP=${5:-0.5}

echo "🚀 Running YCSB Benchmark"
echo "   Threads: $THREADS, Payload: $PAYLOAD, Read: $READ_PROP, Write: $WRITE_PROP"
echo ""

java -Dlog4j.configurationFile=log4j2.xml \
  -DlogFilename=ycsb-$(date +%s).log \
  -cp asd-client.jar site.ycsb.Client \
  -t -s -P config.properties \
  -threads $THREADS \
  -p fieldlength=$PAYLOAD \
  -p hosts=$SERVERS \
  -p readproportion=$READ_PROP \
  -p updateproportion=$WRITE_PROP \
  "$@"

