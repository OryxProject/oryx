In broad terms, Oryx 2 is more extensible and scalable, and built on more modern technology than Oryx 1, which began from a design 4 years old now. It is however, a more complex system to deploy and operate. This page highlights some key differences.

# Oryx 2 Design Goals

1. Provide a more reusable platform for [lambda-architecture](http://lambda-architecture.net/)-style
designs, with batch, speed and serving layers
1. Make each layer usable independently
1. Better support for common machine learning needs
  - Test/train set split and evaluation
  - Parallel model build
  - Hyper-parameter selection
1. Use newer technologies like Spark and Streaming in order to simplify:
  - Remove separate in-core implementations for scale-down
  - Remove custom data transport implementation in favor of [Apache Kafka](http://kafka.apache.org/)
  - Use a 'real' streaming framework instead of reimplementing a simple one
  - Remove complex MapReduce-based implementations in favor of
  [Apache Spark](http://spark.apache.org/)-based implementations
1. Support more input (i.e. not just [CSV](http://en.wikipedia.org/wiki/Comma-separated_values))

# Key Differences

## Architecture

| Oryx 1 | Oryx 2 |
| ------ | ------ |
| One monolithic "tier" for lambda architecture and apps | Three tiers: lambda,  ML, apps |
| No app-level extensibility | Platform for building other lambda- and ML-based apps |
| Two layers: Computation and Serving | Three layers: Batch, Speed and Serving |
| Based on Crunch, MapReduce, HDFS, Tomcat | Based on HDFS, YARN, Spark (+ Streaming, MLlib), Kafka, Zookeeper, Tomcat |
| 27K lines production code / 4K test | 10K lines production code / 7.5K test: simpler, better tested |

## Deployment

| Oryx 1 | Oryx 2 |
| ------ | ------ |
| Requires Java 6, optionally core Hadoop 2.2+ (including "MR1") | Requires Java 7, core Hadoop 2.5+ (YARN, not "MR1") Spark 1.3+, Kafka 0.8.2+, Zookeeper 3.4.5+ |
| Supports local, non-Hadoop deployment  | No non-Hadoop deployment |
| Supports MapReduce-based Hadoop deployment | Supports  only deployment with core Hadoop, YARN, Spark, Kafka |

## Scale and Reliability

| Oryx 1 | Oryx 2 |
| ------ | ------ |
| Memory-efficient | Fast, memory-hungry |
| Custom, best-effort data transport between layers | Reliable data transport via Kafka |
| Custom MapReduce-based algorithm implementations in Computation Layer | Spark Streaming-based batch layer framework and Spark MLlib-based algorithm implementations |
| Custom in-core incremental model update ("speed layer") | Spark Streaming-based distributed model update |

# Migration Guide

The bad news is that no direct migration is possible between Oryx 1 and Oryx 2; they have very different implementations. However, differences in the user- and developer-facing aspects are by design similar or identical.

## REST API

Oryx 2 contains the same set of end-to-end ML applications as Oryx 1, and exposes virtually the same REST API, unchanged. The only significant difference is that there is no longer a `/refresh` endpoint, because it is unnecessary.

## Configuration

Both implementations use a single configuration file parsed by Typesafe Config. The property namespaces are different but there are some similarities. Compare the [Oryx 1 configuration](https://github.com/cloudera/oryx/blob/master/common/src/main/resources/reference.conf) to the [Oryx 2 configuration](https://github.com/OryxProject/oryx/blob/master/oryx-common/src/main/resources/reference.conf) to understand some of the correspondence and difference.

## Data Storage and Transport

In Oryx 1, all data was stored in a series of directories in HDFS. In Oryx 2, data is transported via Kafka (which ultimately stores data in HDFS) and in HDFS as managed by a Spark Streaming process. Although it is still possible to side-load data files via HDFS in Oryx 2, it is not supported and is discouraged, in favor of sending data directly to a Kafka queue.

## Data Formats

In theory, the framework is agnostic to data types and encodings passed between layers. In practice, the provided applications consume the same CSV-encoded data format as Oryx 1.

## Deployment

The deployment requirements are the most different. Although all layers are still distributed as Java `.jar` binaries, now, a Hadoop cluster is required, including HDFS, YARN, Kafka, Spark, and Zookeeper services. Your environment or cluster must be updated to include these services before you can use Oryx 2.

