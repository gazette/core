|circleci status|

.. |circleci status| image:: https://circleci.com/gh/LiveRamp/gazette.svg?style=svg
   :target: https://circleci.com/gh/LiveRamp/gazette

Overview
========

Gazette is infrastructure for building streaming platforms. It consists of a
*broker service* for durable logging and publish/subscribe, and a *consumer
framework* for implementing scaled, stateful, and fault-tolerant streaming
applications in Go.

Broker Service
==============

Brokers serve "Journals", a byte-oriented resource resembling a file. Journals
may be read from an arbitrary offset, they may be appended to (only), and they
may grow to an unbounded length which is unconstrained by broker disk capacity.
Append operations are atomic: a writer is assured that either the entire write
is sequenced into the journal or that none of it is.

The brokers provide global sequencing of client writes to journals, and replicate
those writes to ensure durability. They also serve streamed journal reads, which
begin at any offset and optionally block upon reaching an offset which has not yet
been written (the "write head"). In this way, read operations very much resemble
``tail -c ${my_offset} -f`` operations over files.

Consumers Framework
===================

The consumers framework simplifies the development of highly-available user
applications which "consume" messages streamed from journals. Applications are
empowered to keep substantial amounts of application-defined state in an embedded
database (typically a RocksDB), and the framework manages concerns such as database
replication and recovery, fail-over, and routing. Applications may be very long
lived and scale horizontally.

Design Goals, Comparisions (and Non-Goals)
==========================================

Gazette has influences and shares similarities with a number of other projects.
Its architecture also reflects several departures from the solutions of those
influences.

 * Journals provide globally record ordering, durable storage, and publish/subscribe.

Much like Kakfa, LogDevice, Apache BookKeeper, and others. These properties are
the basic building blocks for assembling platforms composed of streaming,
decoupled, and event-sourced services.

However, where these systems are *record* oriented, journals are *byte* oriented.
They're still eminently suited for streams of records, but responsibility for
representation, packing, and parsing are offloaded from the broker to the client.
This simplifies broker implementation and improves performance, as the broker can
concern itself with additive byte sequences rather than granular messages.

 * Brokers do not provide long-term storage of journal content.

This responsibility is offloaded to a "blob" object store such as S3, Google Cloud
Storage, Azure, HDFS, etc. Use of a separate storage backend stands in contrast to
Kafka, where brokers are responsible for log storage. LogDevice and Apache Pulsar
(with BookKeeper) use a similar technique of decoupling log sequencing from storage.

Separation of storage is motivated by multiple factors. Most importantly, a broker
service like Gazette often supports diametrically opposed use-cases: capturing
critical writes of the system as they occur, and serving highly scaled reads of
historical written data. By decoupling storage, we can separately scale the write
capacity of the system from its read capacity. A second factor is that storage
separation enables taking advantage of services like S3 or GCS, which are highly
elastic and suited for scaled read IOPs, and require no explicit provisioning.

 * Journals, once written, are immutable.

Gazette journals are designed to serve as the long-term system of record for data
within the platform. Journals may be trimmed by removing content from the beginning
or even the middle of the log, but an offset range can never be mutated once
written. This is similar to systems like LogDevice and BookKeeper and distinct
from Kafka, whose brokers implement "compaction" of logs based on a keyed primary
message ID.

Implementing compaction within Gazette brokers is not feasible due to its lack
of access to the structure and semantics of records stored in journals. This
would make implementing a system like Apache Samza on Gazette impossible,
as Samza uses RocksDB locally but utilizes Kafka topics to replicate application
state, and relies on this mechanism to compact replication logs over time.

Instead, Gazette consumers make use an insight that embedded LSM-Tree DBs such
as RocksDB *already* perform regular compaction, and structure their on-disk
state as a series of append-only and immutable files. Rather than replicate and
replay individual key/value operations, Gazette consumers instead observe and
sequence the file operations of the database itself into a "recovery log" journal
which can be pruned over time, and cheaply "tailed" by warm-standbys which replay
the file operations to local disk (and do not otherwise incur any compaction cost).

 * Brokers and Consumers are ephemeral, disposable, and quick to start up.

While they make good use of available local disk, they have no reliance on
persistence of mounted data volumes. From a cold-start, brokers are able to serve
journal read, append, and replication operations without having to first copy
any prior written data. As a trade-off, reads may block until the broker observes
that historical content has been written to the backing blob store.

 * Non-goal: Topics or higher-level organizing concepts.

A common tactic to achieve horizontal scale-out of high volume message flows
is to spread a collection of like messages across a number of "partitions",
which collectively form a "topic". Many systems, like Kafka or Pulsar, provide
a formal representation of topics as an API concern. Gazette does not, and
understands only journals.

Instead, Gazette borrows Kubernetes' notion of "labels", which can be applied
to resources like journals, and "label selectors" which define queries over
declared labels. Topics can informally be implemented as a label and selector
like ``topic=my_logs`` but selectors allow for additional flexible expressions
(eg, ``topic=my_logs, region in (apac, us)``, or ``topic in (my_logs, my_new_logs)``).

 * Simple file-based integration with existing batch processing work-flows.

Spans of journal content (known as "fragments") use a content-addressed naming
convention but otherwise impose no file structure and contain only raw journal
content, optionally compressed. Fragments are also written under predictable
prefixes to the backing blob storage service, which means existing batch
processing work-flows can "integrate" with Gazette by directly reading and
watching for files within the blob store, using a service Amazon SNS to
receive file notifications, or using a library which implements such polling
already (such as Spark DStreams).

 * Fast, zone/rack aware fail-over.

Failure of a broker or consumer process should be detected and fail-over quickly,
and should be tolerant to rack or availability zone failures. Such failures
should not interrupt broker or consumer services for more than the seconds it
takes to detect failure and remove affected members from the topology,
appropriately re-balancing their load.

Brokers are able to immediately serve a newly assigned journal without any
replication delay. Gazette consumers may optionally have a number of "warm
standbys" which locally replicate database file state and can quickly take over.

 * Non-goals: distributed state & consensus.

Gazette uses Etcd v3 as the single source-of-truth for distributed state (eg
current members, journals, and current assignments). Etcd v3 leases are used
to detect process failures, and Gazette employs an "allocator" which solves
for and applies assignment updates via checked Etcd transactions.

 * Non-goals: resource management and job scheduling.

Gazette does not manage workloads or services, such as the provisioning or
scaling of brokers or consumers, and relies on an external orchestration framework
to perform these tasks. The authors use and enthusiastically recommend Kubernetes.

.. _(Deprecated, very out of date) Architecture Overview: docs/architecture_overview.rst

Package Layout
==============

``broker``: API and service implementation for Gazette brokers.

``brokertest``: A drop-in broker for tests making use of Gazette.

``client``: Client-side services and adapters for interacting with Gazette brokers.

``codecs``: Compression codecs understood by Gazette.

``etcdtest``: A drop-in Etcd server for tests making use of Etcd.

``fragment``: Lower-level journal replication protocol, fragment indexing, and blob storage implementations.

``http_gateway``: HTTP broker API, implemented as an adapting gateway atop the gRPC API.

``keepalive``: Simple TCP keep-alive dialer & listener.

``keyspace``: Local, decoded representations of a watched Etcd key/value space.

``mainboilerplate``: Configuration and boilerplate reduction for binaries.

``message``: Message framing, mapping, and decoding support for Gazette consumers.

``metrics``: Prometheus metrics instrumentation.

``protocol``: TODO etc...


