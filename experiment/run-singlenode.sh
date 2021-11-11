#!/bin/bash

nodeIP=10.10.1.3
freshStart=true
stateFolder=/tmp/etcd

export ETCD_THR_FILE=/tmp/throughput.out
export ETCD_LAT_FILE=/tmp/latency.out
export ETCD_BEELOG_ENABLE=false

export ETCD_BEELOG_BATCH_SIZE=1000
export ETCD_BEELOG_CONC_LEVEL=2
export ETCD_BEELOG_LOGS_DIR=/tmp/beelog
export ETCD_BEELOG_PARALLEL_IO=false
export ETCD_BEELOG_SECOND_DISK_LOGS_DIR=/disk2/beelog

export ETCD_DATA_DIR=${stateFolder}/data
export ETCD_WAL_DIR=${stateFolder}/wal
export ETCD_SNAPSHOT_COUNT=1000000000000 # infinite?


if [[ ${freshStart} == "true" ]]; then
  rm -rf ${stateFolder}
fi

./go/src/github.com/Lz-Gustavo/etcd/bin/etcd --name=node0 \
  --listen-peer-urls=http://0.0.0.0:2380 \
  --listen-client-urls=http://0.0.0.0:2379 \
  --advertise-client-urls=http://${nodeIP}:2379 \
  --initial-advertise-peer-urls=http://${nodeIP}:2380 \
  --initial-cluster node0=http://${nodeIP}:2380 \
  --initial-cluster-state=new
