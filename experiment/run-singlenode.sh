#!/bin/bash

nodeIP=155.98.36.32
freshStart=true
stateFolder=/tmp/etcd

export ETCD_THR_FILE=/tmp/throuput.out
export ETCD_BEELOG_ENABLE=false

export ETCD_DATA_DIR=${stateFolder}/data
export ETCD_WAL_DIR=${stateFolder}/wal
export ETCD_SNAPSHOT_COUNT=1000000000000 # infinite?


if [[ ${freshStart} == "true" ]]; then
  rm -r ${stateFolder}
fi

./go/src/github.com/Lz-Gustavo/etcd/bin/etcd --name=node0 \
  --listen-peer-urls=http://0.0.0.0:2380 \
  --listen-client-urls=http://0.0.0.0:2379 \
  --advertise-client-urls=http://${nodeIP}:2379 \
  --initial-advertise-peer-urls=http://${nodeIP}:2380 \
  --initial-cluster node0=http://${nodeIP}:2380 \
  --initial-cluster-state=new
