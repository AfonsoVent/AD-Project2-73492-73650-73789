#!/bin/bash

nthreads=$1
payload=$2
readsper=$3
writesper=$4
processes=$5   # number of server nodes / cluster nodes used

shift 5

nodes=("charmander-5" "oddish-1" "bulbasaur-3")

babelport=34000
base_server_port=35000

if [ -z "$nthreads" ] || [ -z "$payload" ] || [ -z "$readsper" ] || [ -z "$writesper" ] || [ -z "$processes" ]; then
  echo "Usage: $0 nthreads payload reads writes processes [extra YCSB args]"
  exit 1
fi

if [ "$processes" -gt "${#nodes[@]}" ]; then
  echo "Not enough nodes available"
  exit 1
fi

############################
# Build membership string
############################

membership="${nodes[0]}:${babelport}"

i=1
while [ $i -lt $processes ]; do
  membership="${membership},${nodes[$i]}:$((babelport + i))"
  i=$((i + 1))
done

echo "Membership: $membership"

read -p "Press ENTER to start servers..."

############################
# START SERVERS
############################

i=0
while [ $i -lt $processes ]; do
  node=${nodes[$i]}

  ssh $node "
    mkdir -p ~/logs &&
    cd ~/server &&
    nohup java -Xms512m -Xmx2g \
      -DlogFilename=logs/node$((babelport + i)) \
      -cp target/DistAlg.jar Main \
      babel.address=$node \
      babel.port=$((babelport + i)) \
      server_port=$((base_server_port + i)) \
      initial_membership='$membership' \
      > ~/logs/server_$node.log 2>&1 &
  "

  echo "Started server on $node"
  sleep 1
  i=$((i + 1))
done

sleep 5

############################
# START YCSB CLIENTS
############################

echo "Starting YCSB clients..."

i=0
while [ $i -lt $processes ]; do
  node=${nodes[$i]}

  ssh $node "
    cd ~/client &&
    nohup java -Dlog4j.configurationFile=log4j2.xml \
      -DlogFilename=client_$node.log \
      -cp asd-client.jar site.ycsb.Client \
      -t -s -P config.properties \
      -threads $nthreads \
      -p fieldlength=$payload \
      -p hosts='$membership' \
      -p readproportion=$readsper \
      -p updateproportion=$writesper \
      $@ \
      > ~/logs/client_$node.out 2>&1 &
  "

  echo "Started YCSB on $node"
  sleep 1
  i=$((i + 1))
done

echo "All servers and clients started."

############################
# WAIT / STOP SECTION
############################

read -p "Press ENTER to kill everything..."

for node in "${nodes[@]}"; do
  ssh $node "pkill -f DistAlg.jar; pkill -f site.ycsb.Client"
done

echo "All processes terminated."