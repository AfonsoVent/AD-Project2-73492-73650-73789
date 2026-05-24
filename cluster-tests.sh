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

  # Internal Raft membership
  if [ -z "$membership" ]; then
    membership="${ip}:${babel_port}"
  else
    membership="${membership},${ip}:${babel_port}"
  fi

  # Client-visible endpoints
  if [ -z "$client_hosts" ]; then
    client_hosts="${ip}:${server_port}"
  else
    client_hosts="${client_hosts},${ip}:${server_port}"
  fi

done

echo "================================================="
echo "RAFT MEMBERSHIP:"
echo "$membership"
echo
echo "CLIENT HOSTS:"
echo "$client_hosts"
echo "================================================="
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
      -Djava.net.preferIPv4Stack=true \
      -Xms2g \
      -Xmx2g \
      -DlogFilename=logs/node_${babel_port} \
      -cp target/DistAlg.jar Main \
      babel.address=${ip} \
      babel.port=${babel_port} \
      server_port=${server_port} \
      initial_membership='${membership}' \
      > ~/logs/server_${node}.log 2>&1 &
  "

  echo "Started server on ${node}"
  echo "  babel.port=${babel_port}"
  echo "  server_port=${server_port}"

  sleep 2

done

# =========================================================
# WAIT FOR CLUSTER STABILIZATION
# =========================================================
echo
echo "Waiting for Raft cluster stabilization..."
sleep 15

# =========================================================
# START SINGLE YCSB CLIENT
# =========================================================
echo
echo "Starting SINGLE YCSB client..."

client_node=${NODES[0]}

ssh $client_node "
  mkdir -p ~/logs

  cd ~/AD-Project2-73492-73650-73789/distalg-project2-base/client

  nohup java \
    -Djava.net.preferIPv4Stack=true \
    -Dlog4j.configurationFile=log4j2.xml \
    -DlogFilename=client_${client_node}.log \
    -cp asd-client.jar \
    site.ycsb.Client \
    -t \
    -s \
    -P config.properties \
    -threads ${nthreads} \
    -p fieldlength=${payload} \
    -p hosts='${client_hosts}' \
    -p readproportion=${readsper} \
    -p updateproportion=${writesper} \
    > ~/logs/client_${client_node}.log 2>&1 &
"

echo "Started SINGLE YCSB client on ${client_node}"

echo
echo "================================================="
echo "SYSTEM RUNNING"
echo "================================================="
echo
echo "Server logs:"
echo "  ~/logs/server_<node>.log"
echo
echo "Client log:"
echo "  ~/logs/client_${client_node}.log"
echo

# =========================================================
# STOP SECTION
# =========================================================
read -p "Press ENTER to STOP everything..."

for node in "${NODES[@]}"; do

  ssh $node "
    pkill -f DistAlg.jar || true
    pkill -f site.ycsb.Client || true
  "

done

echo
echo "All processes stopped."