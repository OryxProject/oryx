<img align="right" src="https://raw.github.com/wiki/OryxProject/oryx/OryxLogoMedium.png" />

Oryx 2 is a realization of the lambda architecture built on [Apache Spark](http://spark.apache.org) 
and [Apache Kafka](http://kafka.apache.org), but with specialization for real-time large scale machine 
learning. It is a framework for building applications, but also includes packaged, end-to-end 
applications for collaborative filtering, classification, regression and clustering.

Oryx 2 is a rearchitecting and continuation of the original [Oryx 1](http://github.com/cloudera/oryx) project. 
The [Differences from Oryx 1 wiki](https://github.com/OryxProject/oryx/wiki/Differences-From-Oryx-1) describes changes.

# Applications

Just looking to deploy a ready-made, end-to-end application for collaborative filtering, clustering or classification? Easy.
Proceed directly to:

- Prepare your Hadoop cluster with [Cluster Setup](https://github.com/OryxProject/oryx/wiki/Cluster-Setup)
- Get a [Release](https://github.com/OryxProject/oryx/releases)
- Prepare a config file from the [Configuration Reference](https://github.com/OryxProject/oryx/wiki/Configuration-Reference)
- Run the binaries with [Running Oryx](https://github.com/OryxProject/oryx/wiki/Running-Oryx)
- Learn about the REST API endpoints you can call in the [API Endpoint Reference](https://github.com/OryxProject/oryx/wiki/API-Endpoint-Reference)

# Framework

Developers can consume Oryx 2 as a framework for building custom applications as well. 
Following the architecture overview below, proceed to [Making an Oryx App](https://github.com/OryxProject/oryx/wiki/Making-an-Oryx-App) to learn how to create
a new application.

## Overview

Oryx 2 consists of three _tiers_, each of which builds on the one below:

1. A generic lambda architecture tier, providing batch/speed/serving layers, which is not 
specific to machine learning
1. A specialization on top providing ML abstractions for hyperparameter selection, etc.
1. An end-to-end implementation of the same standard ML algorithms as an application
([ALS](http://labs.yahoo.com/files/HuKorenVolinsky-ICDM08.pdf), 
[random decision forests](http://en.wikipedia.org/wiki/Random_forest), 
[k-means](http://en.wikipedia.org/wiki/K-means_clustering)) on top

Viewed another way, it contains the three side-by-side cooperating _layers_ of the lambda 
architecture too, as well as a connecting element:

1. A *Batch Layer*, which computes a new "result" (think model, but, could be anything) 
as a function of all historical data, and the previous result. This may be a long-running operation
which takes hours, and runs a few times a day for example. 
2. A *Speed Layer*, which produces and publishes incremental model updates from a 
stream of new data. These updates are intended to happen on the order of seconds. 
3. A *Serving Layer*, which receives models and updates and implements a synchronous API exposing
query operations on the result.
4. A data transport layer, which moves data between layers and receives input from external sources

The project may be reused tier by tier: for example, the packaged app tier can be ignored, and it 
can be a framework for building new ML applications. It can be reused layer by layer too: 
for example, the Speed Layer can be omitted if a deployment does not need incremental updates.
It can be modified piece-by-piece too: the collaborative filtering application's model-building
batch layer could be swapped for a custom implementation based on a new algorithm outside
Spark MLlib while retaining the serving and speed layer implementations.

<img src="https://raw.github.com/wiki/OryxProject/oryx/Architecture.png"/>

## Module Mapping

Major modules and their relation to tiers and layers:

|          | *Serving*             | *Speed*                     | *Batch*                     |
| --------:| --------------------- | --------------------------- | --------------------------- |
| *Binary* | `oryx-serving`        | `oryx-speed`                | `oryx-batch`                |
| *App*    | `oryx-app-serving`    | `oryx-app-mllib` `oryx-app` | `oryx-app-mllib` `oryx-app` |
| *ML*     |                       | `oryx-ml`                   | `oryx-ml`                   |
| *Lambda* | `oryx-lambda-serving` | `oryx-lambda`               | `oryx-lambda`               |

* The Lambda tier is implemented in `oryx-lambda` for the Speed and Batch layers, since they have much in common, and in the `oryx-lambda-serving` module for the Serving layer because its dependencies are fairly different.
* Likewise `oryx-ml` contains the ML tier's Batch and Speed layer; there is no Serving-specific ML support (now)
* `oryx-app-mllib` and `oryx-app` implement packaged applications' Batch and Speed layers, some parts of which are built on Spark MLlib and some of which are not, respectively. Serving layer application implementations exist separately in `oryx-app-serving`
* `oryx-batch`, `oryx-speed` and `oryx-serving` are simply packaging modules that create finished runnable binaries from the above.
* Support modules like `oryx-common`, `oryx-app-common`, `kafka-util` and `zk-util` are not shown.
* APIs that applications implement to integrate with the ML and Lambda tier are in `oryx-api`; APIs exposed by the packaged apps themselves are in `oryx-app-api`

## Lambda Tier Implementation

### Data transport

The data transport mechanism is an [Apache Kafka](http://kafka.apache.org/) topic. 
Any process -- including but not limited to the serving layer -- can put data onto the topic, 
to be seen by the speed and batch layers. Kafka topics are also used to publish both
*models* and *model updates*, for consumption by the speed and serving layers.

### Batch Layer

The batch layer is implemented as a [Spark Streaming](http://spark.apache.org/streaming/) 
process on a Hadoop cluster, which reads data from the input Kafka topic. The Streaming process 
necessarily has a very long period -- hours or even a day. It uses Spark to save the 
current window of data to HDFS, and then combine with all historical data on HDFS, and 
initiate building of a new result. The result is written to HDFS, and, also published
to a Kafka update topic.

### Speed Layer

The speed layer is implemented as a Spark Streaming process as well, which also listens for
data from the input Kafka topic. It has a much shorter period, on the order of seconds. 
It periodically loads a new model from the update topic and continually produces model updates.
These are put back onto the update topic too.

### Serving Layer

The serving layer listens for model and model updates on the update topic. It maintains model
state in memory. It exposes an HTTP 
[REST](http://en.wikipedia.org/wiki/Representational_state_transfer) API on top of methods 
that query the model in memory. Many of these may be deployed for scale. Each may 
also accept new data and write it to Kafka where it can be seen by all Speed layers.
The different layer defaults are provided by a `reference.conf`. Applications are expected to provide their 
own conf file to specify properties that determine what application resources are required. This can be provided 
using an `application.conf` available in the classpath (which Typesafe Config loads automatically) or 
by providing a command line argument which specifies a config file to load 
(e.g. `-Dconfig.file=<path>/cfg.conf`) or even by specifying individual properties on the command line 
(e.g. `-D<property>=<value>`).

## ML Tier Implementation

The ML tier is simply an implementation and specialization of the generic interfaces mentioned 
above, which implement common ML needs and then expose a different ML-specific interface for
applications to fill in.

For example, it implements a batch layer update process that selects a test and training set
automatically. It calls an app-supplied function to evaluate the model on the test set.
It can automatically repeat this, and with different hyperparameter values, choosing the best
result. It manages serialization of the model via 
[PMML](http://www.dmg.org/v4-2-1/GeneralStructure.html).

## Usage and Deployment

The application is written in Java, using Spark 1.2.x+, 
[Hadoop](http://hadoop.apache.org/) 2.5.x+, [Tomcat](http://tomcat.apache.org/) 8.x+,
Kafka 0.8.2+, [Zookeeper](http://zookeeper.apache.org/) and more. Configuration uses a single 
[Typesafe Config](https://github.com/typesafehub/config) config file, wherein 
applications configure an entire deployment of the system. This includes implementations of 
key interface classes which implement the batch, speed, and serving logic. Applications  
package and deploy their implementations with each instance of the layer binaries. Each 
of these is a runnable Java `.jar` which starts all necessary services.

------

[![Build Status](https://travis-ci.org/OryxProject/oryx.png?branch=master)](https://travis-ci.org/OryxProject/oryx)
[![Coverity](https://scan.coverity.com/projects/2697/badge.svg)](https://scan.coverity.com/projects/2697)
[![codecov.io](https://codecov.io/github/OryxProject/oryx/coverage.svg?branch=master)](https://codecov.io/github/OryxProject/oryx?branch=master)
