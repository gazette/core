#!/usr/bin/env bash
set -Eeu -o pipefail

# Test if minikube is present. If so, we assume we're using it on Linux
# without a VM driver, and with the roles-based auth plugin. Otherwise,
# default to docker for desktop.
if [[ -x "$(which minikube)" ]] ; then
  FLAGS="--vm-driver=none --extra-config=apiserver.authorization-mode=RBAC"

  # TODO(johnny): Remove with https://github.com/kubernetes/kubeadm/issues/845 (K8s v1.11).
  SYSTEMD_RESOLVD_CONF="/run/systemd/resolve/resolv.conf"
  if [[ -f ${SYSTEMD_RESOLVD_CONF} ]]; then
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
$HELM init --service-account tiller --wait

# Add the "incubator" repository, which is relied on for the "incubator/etcd" chart.
$HELM repo add incubator http://storage.googleapis.com/kubernetes-charts-incubator
$HELM repo update

echo "Kubernetes and Tiller are ready. Use context \"$CONTEXT.\""