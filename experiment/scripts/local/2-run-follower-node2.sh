#!/bin/bash

nodeIP=127.0.0.1
freshStart=true

diskpath=/tmp
stateFolder=${diskpath}/etcd

# NOTE: not yet implemented
export RAFT_MEASURE_FOLLOWER_LAG_ENABLED=false
export RAFT_MEASURE_FOLLOWER_LAG_INTERVAL=3s
export RAFT_MEASURE_FOLLOWER_LAG_FILENAME=/tmp/follower-lag.out

export RAFT_MEASURE_FOLLOWER_CATCHUP_ENABLED=true
export RAFT_MEASURE_FOLLOWER_CATCHUP_FILENAME=/tmp/follower-catchup-time.out

# NOTE: not yet implemented
export ETCD_THR_FILE=${measurepath}/throughput.out
export ETCD_LAT_FILE=${measurepath}/latency.out


export ETCD_DATA_DIR=${stateFolder}/data
export ETCD_WAL_DIR=${stateFolder}/wal
export ETCD_SNAPSHOT_COUNT=1000000000000 # infinite?


if [[ ${freshStart} == "true" ]]; then
  rm -rf ${stateFolder}
fi

~/go/src/github.com/Lz-Gustavo/etcd/bin/etcd --name=node2 \
  --log-level=debug \
  --initial-advertise-peer-urls http://${nodeIP}:2382 \
  --listen-peer-urls=http://${nodeIP}:2382 \
  --listen-client-urls=http://${nodeIP}:2372 \
  --advertise-client-urls=http://${nodeIP}:2372 \
  --initial-cluster-token etcd-cluster-1 \
  --initial-cluster node0=http://${nodeIP}:2380,node1=http://${nodeIP}:2381,node2=http://${nodeIP}:2382 \
  --initial-cluster-state=new

