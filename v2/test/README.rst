Notes on setting up and testing via Minikube
============================================

See ``bootstrap_and_run.sh`` for run-able documentation of bootstrapping a local
kubernetes cluster and running examples. While that script can be run directly,
it's recommended to first try running it incrementally and manually to better
understand the steps and what's happening with each.

``bootstrap_and_run.sh`` starts minio with (only) the ``examples`` bucket. If needed,
additional buckets can be created by port-forwarding to the minio service and
using the web UI and the configured access & secret key (typically the defaults
of ``AKIAIOSFODNN7EXAMPLE`` and ``wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY``)::

  alias minio_pod="kubectl get pod -l release=minio -o jsonpath={.items[0].metadata.name}"
  kubectl port-forward $(minio_pod) 9000:9000
  browse http://localhost:9000


TODO Notes on the ``word-count`` application, which need to be cleaned up::

  wget http://www.textfiles.com/etext/AUTHORS/DICKENS/dickens-tale-126.txt
  ADDRESS=http://word-count-svc-address:8080 wordcountctl publish --file dickens-tale-126.txt
  ADDRESS=$CONSUMER_ADDRESS wordcountctl query --prefix "of" --shard shard-000

