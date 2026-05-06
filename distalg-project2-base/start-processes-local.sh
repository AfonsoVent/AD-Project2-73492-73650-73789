#!/bin/bash

processes=$1

if [ -z $processes ] || [ $processes -lt 1 ]; then
  echo "please indicate a number of processes of at least one"
  exit 0
fi

i=0
babelport=34000
base_server_port=35000

membership="localhost:${babelport}"

read -p "------------- Press enter start. After starting, press enter to kill all servers --------------------"

i=1
while [ $i -lt $processes ]; do
    membership="${membership},localhost:$(($babelport + $i))"
    i=$(($i + 1))
done

i=0
while [ $i -lt $processes ]; do
  java -DlogFilename=logs/node$(($babelport + $i)) -cp target/DistAlg.jar Main babel.address=127.0.0.1 babel.port=$(($babelport + $i)) server_port=$(($base_server_port + $i)) initial_membership=$membership 2>&1 | sed "s/^/[$(($babelport + $i))] /" &
  echo "launched process on SMR port $(($babelport + $i)), server port $(($base_server_port + $i))"
  sleep 1
  i=$(($i + 1))
done

sleep 2
read -p "------------- Press enter to kill servers. --------------------"

kill $(ps aux | grep 'DistAlg.jar' | awk '{print $2}')

echo "All processes done!"
