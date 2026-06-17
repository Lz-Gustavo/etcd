#!/bin/bash

nodeIP=127.0.0.1
nodeID=node2

freshStart=true
diskpath=/tmp
stateFolder=${diskpath}/etcd-${nodeID}

# NOTE: not yet implemented
export RAFT_MEASURE_FOLLOWER_LAG_ENABLED=false
export RAFT_MEASURE_FOLLOWER_LAG_INTERVAL=3s
export RAFT_MEASURE_FOLLOWER_LAG_FILENAME=/tmp/follower-lag-${nodeID}.out

export RAFT_MEASURE_FOLLOWER_CATCHUP_ENABLED=false
export RAFT_MEASURE_FOLLOWER_CATCHUP_FILENAME=/tmp/follower-catchup-time-${nodeID}.out

# NOTE: not yet implemented
export ETCD_THR_FILE=${measurepath}/throughput.out
export ETCD_LAT_FILE=${measurepath}/latency.out


# NOTE (Gus): follower artificial latency configured only for node2
export RAFT_LOCAL_FOLLOWER_LATENCY_ENABLED=true
export RAFT_LOCAL_FOLLOWER_LATENCY_DURATION=300ms


export ETCD_DATA_DIR=${stateFolder}/data
export ETCD_WAL_DIR=${stateFolder}/wal
export ETCD_SNAPSHOT_COUNT=1000000000000 # infinite?

# NOTE (Gus): increased values to allow evaluation with artificially increased latency
export ETCD_HEARTBEAT_INTERVAL="500"
export ETCD_ELECTION_TIMEOUT="5000"


if [[ ${freshStart} == "true" ]]; then
  rm -rf ${stateFolder}/data
  rm -rf ${stateFolder}/wal
fi

~/go/src/github.com/Lz-Gustavo/etcd/bin/etcd --name=${nodeID} \
  --log-level=debug \
  --initial-advertise-peer-urls http://${nodeIP}:2382 \
  --listen-peer-urls=http://${nodeIP}:2382 \
  --listen-client-urls=http://${nodeIP}:2372 \
  --advertise-client-urls=http://${nodeIP}:2372 \
  --initial-cluster-token etcd-cluster-1 \
  --initial-cluster node0=http://${nodeIP}:2380,node1=http://${nodeIP}:2381,node2=http://${nodeIP}:2382 \
  --initial-cluster-state=new
