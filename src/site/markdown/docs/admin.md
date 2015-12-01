title: Docs: Admin

# Cluster Setup

The following are required as of Oryx 2.2.0:

- Java 8 or later (JRE only is required)
- A Hadoop cluster running the following components:
    - Apache Hadoop 2.6.0 or later
    - Apache Zookeeper 3.4.5 or later
    - Apache Kafka 0.8.2 or later (in 0.8.x line)
    - Apache Spark 1.5.0 or later

[CDH](http://www.cloudera.com/content/cloudera/en/products-and-services/cdh.html)
5.5.0 and later meet these requirements, although any Hadoop distribution with these
components should work fine. While the rest of the instructions will refer to a CDH 5.5+
distribution, this is not a requirement.

_Note: Oryx 2.0.x requires only Java 7, Spark 1.3.0 and CDH 5.4.x_
_Note: Oryx 2.1.x requires only Java 7, Spark 1.5.0 and CDH 5.5.x_

A single-node cluster can be sufficient, although running all of these components on one machine
may require a reasonable amount of RAM.

## Services

Install and configure the Hadoop cluster normally. The following services need to be enabled:

- HDFS
- YARN
- Zookeeper
- Kafka
- Spark (on YARN)

Note that for CDH, Kafka is available as a parcel from
[Cloudera Labs](http://www.cloudera.com/content/cloudera/en/developers/home/cloudera-labs/apache-kafka.html).

Determine the (possibly several) Kafka brokers that are configured in the cluster, under Instances,
and note their hosts and port. The port is typically 9092. Same for the Zookeeper servers; the default
port here is 2181. Default ports will be used in subsequent examples.

Where a Kafka broker or Zookeeper server is called for, you can and should specify a comma-separated
list of `host:port` pairs where there are multiple hosts. Example: `your-zk-1:2181,your-zk-2:2181`.

Also note whether your Zookeeper instance is using a chroot path. This is simply a path suffixed
to the `host:port`, like `your-zk:2181/your-chroot`. It is often `/kafka` if it is set.
You can omit this if you are not using a chroot. Note: if you have multiple Zookeeper servers, 
and a chroot, only add the chroot once, at the end: `your-zk-1:2181,your-zk-2:2181/kafka`

## Configuring Kafka

Oryx will use two Kafka topics for data transport. One carries input data to the batch and
Speed Layer, and the other carries model updates from there on to the Serving Layer. The default
names of these topics are `OryxInput` and `OryxUpdate` respectively. They need to be
created before Oryx is started.

The number of partitions for the _input_ topic will affect the number of partitions, and 
therefore parallelism, of the Spark Streaming jobs that consume them. For example, the
Batch Layer reads partitions of historical data from HDFS and from Kafka. If the input
topic has just one partition but a large amount of data arrives per interval, then the
Kafka-based partition of the input may be relatively very large and take a long time 
to process. A good rule of thumb may be to choose a number of topic partitions such that the
amount of data that arrives in one batch interval is expected to be about the same as
one HDFS block, which is 128MB by default.

The provided `oryx-run.sh kafka-setup` script configures a default of 4 partitions, but
this can be changed later. Note that there is no purpose in configuring more than 1
partition for the _update_ topic.

Replication factor can be any value, but at least 2 is recommended.
Note that the replication factor can't exceed the number of Kafka brokers in the cluster.
The provided script sets replication to 1, by default, for this reason.
This can be changed later with, for example, 
`kafka-topics --zookeeper ... --alter --topic ... --replication-factor N`

You may need to configure the retention time for one or both topics. In particular,
it's typically important to limit the retention time for the update topic, since the Speed
and Serving Layer read the entire topic from the start on startup to catch up. 
This is not as important for the input topic, which is not re-read from the beginning.

Setting it
to twice the Batch Layer update interval is a good start. For example, to set it to 1 day
(24 * 60 * 60 * 1000 = 86400000 ms), set the topic's `retention.ms` property to 86400000.
This is done automatically by the provided `oryx-run.sh kafka-setup` script.

The two topics above may contain large messages; in particular the update topic includes 
entire serialized PMML models. It's possible that they exceed Kafka's default max message 
size of 1 MiB. If large models are expected, then the topic's `max.message.bytes` should
be configured to allow larger messages. `oryx-run.sh kafka-setup` sets a default of 16 MiB
for the update topic. This is also the default maximum size of a model that Oryx will attempt
to write to the update topic; larger models will be passed as a reference to the model
file's location on HDFS instead. See setting `oryx.update-topic.message.max-size`.

The Kafka broker's `message.max.bytes` (note the different name!) property also controls 
this, but setting it affects all topics managed by the broker, which may be undesirable.
See [Performance and Resource Considerations](http://www.cloudera.com/content/cloudera/en/documentation/cloudera-kafka/latest/topics/kafka_performance.html)
for a more complete discussion. In particular, note that `replica.fetch.max.bytes` would
have to be set in the broker in order to _replicate_ any very large messages. There is no
per-topic equivalent to this.

## Automated Configuration

The provided `oryx-run.sh` script can be used to print current configuration for Zookeeper,
list existing topics in Kafka, and optionally create the configured input and update topics
if needed. 

You will need to create an Oryx configuration file first, which can be cloned from the example at
[conf/als-example.conf](https://github.com/OryxProject/oryx/blob/master/app/conf/als-example.conf)
as a start. At least change the Kafka and Zookeeper configuration, as well as topic names, as desired.

With this file as `oryx.conf` and any of the layer JAR files in the same directory, run:

```bash
./oryx-run.sh kafka-setup

Input  ZK:    your-zk:2181
Input  Kafka: your-kafka:9092
Input  topic: OryxInput
Update ZK:    your-zk:2181
Update Kafka: your-kafka:9092
Update topic: OryxUpdate

All available topics:


Input topic OryxInput does not exist. Create it? y
Creating topic OryxInput
Created topic "OryxInput".
Status of topic OryxInput:
Topic:OryxInput	PartitionCount:4	ReplicationFactor:1	Configs:
	Topic: OryxInput	Partition: 0	Leader: 120	Replicas: 120,121	Isr: 120,121
	Topic: OryxInput	Partition: 1	Leader: 121	Replicas: 121,120	Isr: 121,120
	Topic: OryxInput	Partition: 2	Leader: 120	Replicas: 120,121	Isr: 120,121
	Topic: OryxInput	Partition: 3	Leader: 121	Replicas: 121,120	Isr: 121,120

Update topic OryxUpdate does not exist. Create it? y
Creating topic OryxUpdate
Created topic "OryxUpdate".
Updated config for topic "OryxUpdate".
Status of topic OryxUpdate:
Topic:OryxUpdate	PartitionCount:1	ReplicationFactor:1	Configs:retention.ms=86400000,max.message.bytes=16777216
	Topic: OryxUpdate	Partition: 0	Leader: 120	Replicas: 120,121	Isr: 120,121
```

To watch messages sent to the input and update topics, to monitor action of the application,
try:

```bash
./oryx-run.sh kafka-tail
Input  ZK:    your-zk:2181
Input  Kafka: your-kafka:9092
Input  topic: OryxInput
Update ZK:    your-zk:2181
Update Kafka: your-kafka:9092
Update topic: OryxUpdate

...output...
```

Then in another window, you can feed input, such as the `data.csv` example from the 
[end user docs](endusers.html), into the input queue to verify it's working with:

```bash
./oryx-run.sh kafka-input --input-file data.csv
```

If all is well, these processes can be terminated. The cluster is ready to run Oryx.


# Handling Failure

Eventually, you'll want to stop one or more of the Layers running, or restart it. Or maybe
a server decides to die. What happens then? What's the worst that can happen?

## Data Loss

Historical data is saved in HDFS, which should be configured for replication. HDFS ensures
data is stored reliably. Kafka is also designed to cope with failure when configured to use
replication.

That is, there is nothing special to do here in order to ensure that data is
never completely lost. It is the job of HDFS and Kafka to always be available and not lose
data.

## Server Failure

In general, all three Layer server processes should run continuously, and can and should be
restarted immediately if they have to be stopped, or in case of a failure.
This can be accomplished with an init script or similar mechanism (not included, yet).

### Serving Layer

The Serving Layer has no state. On startup, it reads all models and updates available on the
update topic. It begins answering queries as soon as any first, valid model is
available. For this reason, it's desirable to limit the retention time for the update topic.

The operation of the Serving Layer is not distributed. Each instance is independent, and may
stop or start without affecting others.

### Speed Layer

The Speed Layer also has no state, and also reads all models and updates available on the
update topic. It begins producing updates as soon as it has a valid model. It also begins
reading from the input topic, and at the moment, always reads from the latest offset.

The Speed Layer uses Spark Streaming and Spark for some of its computation. Spark has
the responsibility of dealing with failures during computation in the cluster and retrying
tasks.

Spark Streaming's Kafka integration can in some cases recover from failure of the receiver
that is reading from Kafka. If the entire process dies and is restarted, and `oryx.id` has
been set, then reading will be able to resume from the last offset recorded by Kafka.
(Otherwise, it will resume reading from the latest offset. This means data that arrived
while no Speed Layer was running will not have produced any update.) Also, data that arrives
before the Speed Layer has a model is ignored too. It effectively adopts "at most once"
semantics.

Because the role of the Speed Layer is to provide an approximate, "best effort" update to the
last published model, this behavior is generally no problem, and desirable because of its
simplicity.

### Batch Layer

The Batch Layer is the most complex, since it does generate some state:

- Historical data, is always persisted to HDFS
- If the app chooses to, additional state like models can be persisted to HDFS as well as topics

It also is most sensitive to reading data multiple times or not at all, since it is the component
that creates the "official" next model.

As with the Speed Layer, Spark and Spark Streaming handles many of the failure scenarios during
computation. It also manages storing data to HDFS and is responsible for avoiding writing the
same data twice.

Applications are responsible for recovering their own 'state'; currently,
applications built on the Oryx ML tier write state into unique subdirectories,
and will simply produce a new set of state in a new directory when restarted.
Previous state, if it exists, will have been completely written or not at all.

The Batch Layer also currently adopts the same "at most once" semantics as the Speed Layer.
As above, if the entire process dies and is restarted, and `oryx.id` has
been set, then reading will be able to resume from the last offset recorded by Kafka,
and otherwise, it will resume reading from the latest offset.

# Troubleshooting / FAQ

## Unsupported major.minor version 51.0

This means you are running Java 6 somewhere. Oryx 2 requires Java 7 or later.

## Initial job has not accepted any resources

The error usually means that your YARN cluster can't allocate the resources (memory, cores) 
that your application is requesting. You'll have to check and increase what YARN can allocate, 
free up room, or decrease the amount that your app asks for.

The relevant YARN settings are:

- Container Memory (`yarn.nodemanager.resource.memory-mb`) - the maximum memory that one YARN node has to allocate to containers
- Container Virtual CPU Cores (`yarn.nodemanager.resource.cpu-vcores`) - same, for cores
- Container Memory Maximum (`yarn.scheduler.maximum-allocation-mb`) - maximum memory for one container
- Container Virtual CPU Cores Maximum (`yarn.scheduler.maximum-allocation-vcores`) - maximum cores for one container

The relevant app settings are:

- `oryx.{batch,speed}.streaming.num-executors` - number of executors (YARN containers) to allocate
- `oryx.{batch,speed}.streaming.executor-cores` - cores to allocate per executor
- `oryx.{batch,speed}.streaming.executor-memory` - memory to allocate per executor

## Required executor memory (... MB) is above the max threshold (... MB) of this cluster

This means your YARN configuration limits the maximum container size that can be requested.
Increase the Container Memory Maximum (`yarn.scheduler.maximum-allocation-mb`)
to something larger. For Spark, it generally makes sense to allow large containers.

## IllegalArgumentException: Wrong FS

```
java.lang.IllegalArgumentException: Wrong FS: hdfs:..., expected: file:///
    	at org.apache.hadoop.fs.FileSystem.checkPath(FileSystem.java:645)
```

This typically means you are using HDFS, but your Hadoop config (e.g. `core-site.xml`, typically in
`/etc/hadoop/conf` is not on the classpath. If you're building a custom `compute-classpath.sh` script
make sure to include this directory along with JARs.

## I need to purge all previous data and start again

Input data exists in the input Kafka topic for a time before being copied into HDFS. So,
input potentially exists as unread message in this topic as well as in the HDFS directory
defined by `orxy.batch.storage.data-dir`. It's easy to delete the data in HDFS; it's harder
to ensure older data in the input topic is not read.

The simplest solution is to create a new input topic and change configuration to use it.
Then, also delete any pre-existing data in HDFS (or use a new directory). Similarly, since the 
update topic is read from the beginning, it's easiest to make a new update topic instead.
While it's possible to reuse the existing topics by carefully managing offsets in Kafka or
changing the instance `oryx.id` value, these are possibly more complex.

# Performance

See [the performance doc](performance.html).

# Differences from Oryx 1

Design goals for Oryx 2 were:

- Provide a more reusable platform for [lambda-architecture](http://lambda-architecture.net/)-style
designs, with batch, speed and serving layers
- Make each layer usable independently
- Better support for common machine learning needs
  - Test/train set split and evaluation
  - Parallel model build
  - Hyper-parameter selection
- Use newer technologies like Spark and Streaming in order to simplify:
  - Remove separate in-core implementations for scale-down
  - Remove custom data transport implementation in favor of [Apache Kafka](http://kafka.apache.org/)
  - Use a 'real' streaming framework instead of reimplementing a simple one
  - Remove complex MapReduce-based implementations in favor of
  [Apache Spark](http://spark.apache.org/)-based implementations
- Support more input (i.e. not just [CSV](http://en.wikipedia.org/wiki/Comma-separated_values))

## Architecture Differences

| Oryx 1 | Oryx 2 |
| ------ | ------ |
| One monolithic "tier" for lambda architecture and apps | Three tiers: lambda,  ML, apps |
| No app-level extensibility | Platform for building other lambda- and ML-based apps |
| Two layers: Computation and Serving | Three layers: Batch, Speed and Serving |
| Based on Crunch, MapReduce, HDFS, Tomcat | Based on HDFS, YARN, Spark (+ Streaming, MLlib), Kafka, Zookeeper, Tomcat |
| 27K lines production code / 4K test | 10K lines production code / 7.5K test: simpler, better tested |

## Deployment Differences

| Oryx 1 | Oryx 2 |
| ------ | ------ |
| Requires Java 6, optionally core Hadoop 2.2+ (including "MR1") | Requires Java 7, core Hadoop 2.5+ (YARN, not "MR1") Spark 1.3+, Kafka 0.8.2+, Zookeeper 3.4.5+ |
| Supports local, non-Hadoop deployment  | No non-Hadoop deployment |
| Supports MapReduce-based Hadoop deployment | Supports  only deployment with core Hadoop, YARN, Spark, Kafka |

## Scale and Reliability Differences

| Oryx 1 | Oryx 2 |
| ------ | ------ |
| Memory-efficient | Fast, memory-hungry |
| Custom, best-effort data transport between layers | Reliable data transport via Kafka |
| Custom MapReduce-based algorithm implementations in Computation Layer | Spark Streaming-based batch layer framework and Spark MLlib-based algorithm implementations |
| Custom in-core incremental model update ("speed layer") | Spark Streaming-based distributed model update |

## Migration

The bad news is that no direct migration is possible between Oryx 1 and Oryx 2; they have 
very different implementations. However, differences in the user- and developer-facing aspects 
are by design similar or identical.

### REST API

Oryx 2 contains the same set of end-to-end ML applications as Oryx 1, and exposes virtually 
the same REST API, unchanged. The only significant difference is that there is no longer 
a `/refresh` endpoint, because it is unnecessary.

### Configuration

Both implementations use a single configuration file parsed by Typesafe Config.
The property namespaces are different but there are some similarities. Compare the 
[Oryx 1 configuration](https://github.com/cloudera/oryx/blob/master/common/src/main/resources/reference.conf) 
to the 
[Oryx 2 configuration](https://github.com/OryxProject/oryx/blob/master/framework/oryx-common/src/main/resources/reference.conf) 
to understand some of the correspondence and difference.

### Data Storage and Transport

In Oryx 1, all data was stored in a series of directories in HDFS. In Oryx 2, data is transported 
via Kafka (which ultimately stores data in HDFS) and in HDFS as managed by a Spark Streaming 
process. Although it is still possible to side-load data files via HDFS in Oryx 2, it is not 
supported and is discouraged, in favor of sending data directly to a Kafka queue.

### Data Formats

In theory, the framework is agnostic to data types and encodings passed between layers. 
In practice, the provided applications consume the same CSV-encoded data format as Oryx 1.

### Deployment

The deployment requirements are the most different. Although all layers are still distributed 
as Java `.jar` binaries, now, a Hadoop cluster is required, including HDFS, YARN, Kafka, Spark, 
and Zookeeper services. Your environment or cluster must be updated to include these services 
before you can use Oryx 2.
