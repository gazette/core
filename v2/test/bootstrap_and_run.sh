#!/bin/bash
set -Eeuxo pipefail

# Base V2 directory which parents this script.
V2DIR="$(cd "$(dirname "$0")/.." && pwd)"

# Test if minikube is present. If so, we assume we're using it on Linux
# without a VM driver, and with the roles-based auth plugin. Otherwise,
# default to docker for desktop.
if [ -x "$(which minikube)" ] ; then
  FLAGS="--vm-driver=none --extra-config=apiserver.authorization-mode=RBAC"

  # TODO(johnny): Remove with https://github.com/kubernetes/kubeadm/issues/845 (K8s v1.11).
  SYSTEMD_RESOLVD_CONF="/run/systemd/resolve/resolv.conf"
  if [ -f ${SYSTEMD_RESOLVD_CONF} ]; then
    FLAGS="$FLAGS --extra-config=kubelet.resolv-conf=${SYSTEMD_RESOLVD_CONF}"
  fi

  sudo \
    MINIKUBE_WANTKUBECTLDOWNLOADMSG=false \
    MINIKUBE_WANTNONEDRIVERWARNING=false \
    CHANGE_MINIKUBE_NONE_USER=true \
    minikube start ${FLAGS}

  CONTEXT="minikube"
else
    CONTEXT="docker-for-desktop"
fi

KUBECTL="kubectl --context=${CONTEXT}"
HELM="helm --kube-context=${CONTEXT}"

# Create a service account for helm's server component "tiller" to use, with the cluster-admin role.
$KUBECTL create serviceaccount -n kube-system tiller
$KUBECTL create clusterrolebinding tiller-binding --clusterrole=cluster-admin --serviceaccount kube-system:tiller
$KUBECTL create clusterrolebinding fixDNS --clusterrole=cluster-admin --serviceaccount=kube-system:kube-dns

# Install tiller into the Kubernetes cluster, and wait for its pod to startup.
$HELM init --service-account tiller

until $HELM version; do
  echo "Waiting for Tiller to start..."
  sleep 1
done
$HELM repo update

# Install the "etcd-operator" chart (https://coreos.com/blog/introducing-the-etcd-operator.html).
# etcd-operator provisions Etcd clusters defined as Kubernetes "custom resources".
$HELM install --wait stable/etcd-operator --name etcd-operator

until $KUBECTL get crd etcdclusters.etcd.database.coreos.com; do
  echo "Waiting for etcd-operator to start..."
  sleep 1
done

# Create a custom resource definition for a shared Etcd cluster.
# Etcd-operator will provision it with a K8 service "etcd-cluster-client".
$KUBECTL create --filename - << EOF
apiVersion: "etcd.database.coreos.com/v1beta2"
kind: "EtcdCluster"
metadata:
  name: "etcd-cluster"
spec:
  size: 3
  version: "3.3.9"
EOF

# Install the "minio" chart, which provides an S3-compatible cloud filesystem.
$HELM install --wait stable/minio --name minio  --values /dev/stdin << EOF
defaultBucket:
  enabled: true   # Ask the minio chart to create a bucket.
  name: examples
  policy: public  # One of none|download|upload|public. TODO(johnny): is public required?
persistence:
  enabled: false  # Bucket data is ephemeral, and lives only with the minio pod.
EOF

# Install the "gazette" chart.
$HELM install --wait $V2DIR/charts/gazette --name gazette --values /dev/stdin << EOF
etcd:
  endpoint: "http://etcd-cluster-client:2379"
aws:
  # Use the default Minio access key & secret key fixtures.
  accessKeyID: AKIAIOSFODNN7EXAMPLE
  secretKey: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
EOF

# Create aliases for querying a Gazette service IP address.
function svcAddr {
    host=$($KUBECTL get svc -l release=$1 -o jsonpath={.items[0].spec.clusterIP})
    port=$($KUBECTL get svc -l release=$1 -o jsonpath={.items[0].spec.ports[0].port})
    echo "http://$host:$port"
}

# We'll run gazctl in an ephemeral docker container which has direct access to
# the kubernetes networking space, and mounts this "test" directory. The
# alternative is to run gazctl on the host port-forward broker or consumer pods
# as required to expose the service (yuck).
GAZCTL="docker run \
    --rm \
    --interactive \
    --env BROKER_ADDRESS \
    --env CONSUMER_ADDRESS \
    --volume $V2DIR/test:/test:ro \
    gazette:latest \
    gazctl"

# Create all test journals.
BROKER_ADDRESS=$(svcAddr gazette) $GAZCTL journals apply --specs /test/journalspace.yaml

# Install the "stream-sum" chart.
$HELM install --wait $V2DIR/charts/examples/stream-sum --name stream-sum --values /dev/stdin << EOF
etcd:
  endpoint: "http://etcd-cluster-client:2379"
gazette:
  endpoint: "http://gazette-gazette:8080"
chunker:
  numJobs: 3
  numStreams: 1000
  chunksPerStream: 100
EOF

# Enumerate all stream-sum shards, one for each journal of the topic.
function streamSumShards {
    # Create one shard for each journal & recoverylog hard-coded in test/journalspace.yaml
    # TODO(johnny): This is hacky, but works until we can define better tooling.
    for i in $(seq -f "%03g" 0 7); do
       cat<<EOF
# Define ShardSpec in YAML format. Compare to ShardSpec for field definitions.
- id: chunks-part-$i
  sources:
  - journal: examples/stream-sum/chunks/part-$i
  recovery_log: examples/stream-sum/recovery-logs/shard-chunks-$i
  hint_keys:
  - /gazette/hints/examples/streams-sum/part-$i.recorded
  - /gazette/hints/examples/streams-sum/part-$i.recovered-1
  - /gazette/hints/examples/streams-sum/part-$i.recovered-2
  max_txn_duration: 1s
  disable: false
  hot_standbys: 1
EOF
    done
}

# Create shards by piping the enumeration to `gazctl shards apply` with the stream-sum service address.
streamSumShards | CONSUMER_ADDRESS=$(svcAddr stream-sum) $GAZCTL shards apply --specs /dev/stdin

# Install the "word-count" chart.
$HELM install --wait $V2DIR/charts/examples/word-count --name word-count --values /dev/stdin << EOF
etcd:
  endpoint: "http://etcd-cluster-client:2379"
gazette:
  endpoint: "http://gazette-gazette:8080"
EOF

# Enumerate all word-count shards.
function wordCountShards {
    # Create one shard for journals & recoverylog hard-coded in test/journalspace.yaml
    for i in $(seq -f "%03g" 0 3); do
       cat<<EOF
# Define ShardSpec in YAML format. Compare to ShardSpec for field definitions.
- id: shard-$i
  sources:
  - journal: examples/word-count/deltas/part-$i
  - journal: examples/word-count/relocations/part-$i
  recovery_log: examples/word-count/recovery-logs/shard-$i
  hint_keys:
  - /gazette/hints/examples/word-count/part-$i.recorded
  - /gazette/hints/examples/word-count/part-$i.recovered-1
  - /gazette/hints/examples/word-count/part-$i.recovered-2
  max_txn_duration: 1s
  disable: false
  hot_standbys: 1
EOF
    done
}

# Create shards by piping the enumeration to `gazctl shards apply` with the word-count service address.
wordCountShards | CONSUMER_ADDRESS=$(svcAddr word-count) $GAZCTL shards apply --specs /dev/stdin
