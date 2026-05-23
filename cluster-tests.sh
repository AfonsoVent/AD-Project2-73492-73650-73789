#!/bin/bash

# =========================================================
# Arguments
# =========================================================
processes=$1
nthreads=$2
payload=$3
readsper=$4
writesper=$5

shift 5

nodes_csv=$1
shift 1

if [ -z "$processes" ] || \
   [ -z "$nthreads" ] || \
   [ -z "$payload" ] || \
   [ -z "$nodes_csv" ] || \
   [ -z "$readsper" ] || \
   [ -z "$writesper" ]; then

  echo "Usage:"
  echo "$0 processes threads payload readProp writeProp node1,node2,node3"
  exit 1
fi

# =========================================================
# Parse nodes
# =========================================================
IFS=',' read -ra NODES <<< "$nodes_csv"

if [ "$processes" -gt "${#NODES[@]}" ]; then
  echo "Not enough nodes provided"
  exit 1
fi

# =========================================================
# Build memberships
# =========================================================
membership=""
client_hosts=""

for ((i=0; i<processes; i++)); do

  node=${NODES[$i]}

  ip=$(ssh $node "hostname -I | awk '{print \$1}'")

  babel_port=$((34000 + i))
  server_port=$((35000 + i))

  if [ -z "$membership" ]; then
    membership="${ip}:${babel_port}"
    client_hosts="${ip}:${server_port}"
  else
    membership="${membership},${ip}:${babel_port}"
    client_hosts="${client_hosts},${ip}:${server_port}"
  fi

done

echo "RAFT MEMBERSHIP:"
echo "$membership"
echo
echo "CLIENT HOSTS:"
echo "$client_hosts"
echo

read -p "Press ENTER to start servers..."

# =========================================================
# START SERVERS
# =========================================================
for ((i=0; i<processes; i++)); do

  node=${NODES[$i]}
  ip=$(ssh $node "hostname -I | awk '{print \$1}'")

  babel_port=$((34000 + i))
  server_port=$((35000 + i))

  ssh $node "
    mkdir -p ~/logs
    cd ~/AD-Project2-73492-73650-73789/distalg-project2-base

    nohup java \
      -Xms512m -Xmx2g \
      -Djava.net.preferIPv4Stack=true \
      -DlogFilename=logs/node_${babel_port} \
      -cp target/DistAlg.jar Main \
      babel.address=${ip} \
      babel.port=${babel_port} \
      server_port=${server_port} \
      initial_membership='${membership}' \
      > ~/logs/server_${node}.log 2>&1 &
  "

  echo "Started server on $node ($ip)"
  sleep 2

done

echo "Waiting for cluster..."
sleep 15

# =========================================================
# START CLIENTS
# =========================================================
for ((i=0; i<processes; i++)); do

  node=${NODES[$i]}

  ssh $node "
    mkdir -p ~/logs
    cd ~/AD-Project2-73492-73650-73789/distalg-project2-base/client

    nohup java \
      -Dlog4j.configurationFile=log4j2.xml \
      -DlogFilename=client_${node}.log \
      -cp asd-client.jar \
      site.ycsb.Client \
      -t -s \
      -P config.properties \
      -threads ${nthreads} \
      -p fieldlength=${payload} \
      -p hosts='${client_hosts}' \
      -p readproportion=${readsper} \
      -p updateproportion=${writesper} \
      > ~/logs/client_${node}.log 2>&1 &
  "

  echo "Started client on $node"
  sleep 1

done

echo
echo "SYSTEM RUNNING"
echo

read -p "Press ENTER to STOP everything..."

for node in "${NODES[@]}"; do
  ssh $node "pkill -f DistAlg.jar || true; pkill -f site.ycsb.Client || true"
done

echo "Stopped."