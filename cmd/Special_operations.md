Shard repartitioning
====================

The amount of data that a consumer process has to process is determined by the size of the partitions of the Gazette topic it is reading from. Each partition defines one consumer shard, and you can't process less than one shard at a time. Consequently, it is very important to get the initial partitioning of your topics right from the start. Partitions should be written to evenly so that they are approximately the same size, and there should be enough of them that they don't receive new writes at such a high rate that consumers following along cannot keep up. Over time, the requirements of consumers may change and the amount of data written to a topic may increase as applications scale. It is often necessary to readjust the way that data is partitioned across topic partitions and consumer shards after they have been begun running in production. Without repartitioning, you may find that the keyspace a consumer shard has to deal with grows too large to comfortably fit on disk, or that a consumer shard becomes unable to keep up with the rate of new writes to a topic partition.

Repartitioning new writes to a topic is easy: you just have to adjust the way the process performing writes routes keys to partitions. But active consumers may already have a large amount of state built up, and repartitioning a topic may mean that some of their keys now properly belong to other shards. Resharding that historical data won't happen by itself, and it requires special attention.

This resharding can be accomplished by combining a few commands given in gazctl to perform a map-reduce on the shards. The steps are, essentially, 
* recover existing shards into local rocks databases
* map data in existing shards to a new set of shards
* stitch together all data that belongs to a given shard into a new rocks database
* write a recovery log and hints for each recomposed shard

This produces a set of recovery logs that allow you to spin up a new consumer that will seamlessly continue processing from a repartitioned source topic.

### Recover shards into local rocks databases

Before you start operating on shards, you want a stable snapshot of the state of each shard database at a given time. `gazctl shard recover` provides this for you -- it takes a recovery and an optional hints file and replays it into a local rocks database.

### Map data to new shards

gazctl shard split provides this functionality. You can customize how your data is split into new shards by providing a `shard_ctl` plugin that implements the Split type. The output will be multiple text files consisting of hex-encoded key-value pairs, each holding some fragment of the keyspace.

### Stitch together new shards

If you are just splitting up shards independently, not rearranging data between shards, you can skip this step. However, you may have a situation where you have multiple files that you want to combine into a single rocks database. gazctl shard compose allows you to take multiple input sources and combine them. If multiple input files may share some of the keys, you can also customize how they are merged into a single value by implementing the Merge type in the `shard_ctl` plugin

### Write new recovery logs

`gazctl shard compose` will handle this automatically for you, even if your shards only have a single input source. Just use the `recovery-log` option and it will write a recovery log replaying the rocksdb file operations needed to quickly reconstruct your output shard. Then you can stand up a new set of production consumer instances for these new shards that will smoothly resume from your new source partitions and recovery logs
