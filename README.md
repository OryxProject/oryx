Overview
========

<img align="right" src="https://raw.github.com/wiki/OryxProject/oryx/OryxLogoSmall.png"/>

This is a redesign of the [Oryx](https://github.com/cloudera/oryx) project as "Oryx 2.0". 
The primary design goals are:

1. A more reusable platform for [lambda-architecture](http://lambda-architecture.net/)-style 
designs, with batch, speed and serving layers
1. Make each layer usable independently
1. Fuller support for common machine learning needs
  - Test/train set split and evaluation
  - Parallel model build
  - Hyper-parameter selection
1. Use newer technologies like Spark and Streaming in order to simplify:
  - Remove separate in-core implementations for scale-down
  - Remove custom data transport implementation in favor of message queues like 
  [Apache Kafka](http://kafka.apache.org/)
  - Use a 'real' streaming framework instead of reimplementing a simple one
  - Remove complex MapReduce-based implementations in favor of 
  [Apache Spark](http://spark.apache.org/)-based implementations
1. Support more input (i.e. not just [CSV](http://en.wikipedia.org/wiki/Comma-separated_values))

Architecture
============

The project will produce three "tiers", each of which builds on the last:

1. A generic lambda architecture tier, providing batch/speed/serving layers, which is not 
specific to machine learning
1. A specialization on top providing ML abstractions for hyperparameter selection, etc.
1. An end-to-end implementation of the same standard ML algorithms as an application
([ALS](labs.yahoo.com/files/HuKorenVolinsky-ICDM08.pdf), 
[random decision forests](http://en.wikipedia.org/wiki/Random_forest), 
[k-means++](http://en.wikipedia.org/wiki/K-means_clustering)) on top

The lambda architecture tier is most fundamental, and itself has three standard components and
one connecting component:

1. A Batch Layer, which computes a new "result" (think model, but, could be anything) 
as a function of all historical data, and the previous result. This may be a long-running operation
which takes hours, and runs a few times a day for example. 
2. A Speed Layer, which produces and publishes incremental model updates from a 
stream of new data. These updates are intended to happen on the order of seconds. 
3. A Serving Layer, which receives models and updates and implements a synchronous API exposing
query operations on the result.
4. A data transport layer, which moves data between layers and receives input from external sources

<img src="https://raw.github.com/wiki/OryxProject/oryx/Architecture.png"/>

Availability
------------

|          | *Serving*      | *Speed*      | *Batch*      |
| --------:| -------------- | ------------ | ------------ |
| *ML app* | _no_           | _no_         | alpha        |
| *ML*     | _no_           | _no_         | alpha        |
| *Lambda* | _no_           | _no_         | alpha        |

Module Mapping
--------------

|                   | *Serving*               | *Speed*                         | *Batch*                         |
| -----------------:| ----------------------- | ------------------------------- | ------------------------------- |
| *Packaged binary* | `oryx-serving`          | `oryx-speed`                    | `oryx-batch`                    |
| *ML app*          | `oryx-ml-oryx-serving`  | `oryx-ml-mllib`, `oryx-ml-oryx` | `oryx-ml-mllib`, `oryx-ml-oryx` |
| *ML*              |                         | `oryx-ml`                       | `oryx-ml`                       |
| *Lambda*          | `oryx-lambda-serving`   | `oryx-lambda`                   | `oryx-lambda`                   |


Lambda Tier Implementation
==========================

Data transport
--------------

The data transport mechanism is an [Apache Kafka](http://kafka.apache.org/) queue. 
Any process -- including but not limited to the serving layer -- can put data onto the queue, 
to be seen by the speed and batch layers. Kafka queues are also used to publish both
*models* and *model updates*, for consumption by the speed and serving layers.

Batch Layer
-----------

The batch layer is implemented as a [Spark Streaming](http://spark.apache.org/streaming/) 
process on a Hadoop cluster, which reads data from the input Kafka queue. The Streaming process 
necessarily has a very long period -- hours or even a day. It uses Spark to save the 
current window of data to HDFS, and then combine with all historical data on HDFS, and 
initiate building of a new result. The result is written to HDFS, and, also published
to a Kafka queue.

Speed Layer
-----------

The speed layer is implemented as a Spark Streaming process as well, which also listens for
data from the input Kafka queue. It has a much shorter period, on the order of seconds. 
It periodically loads a new model from the model queue and continually produces model updates.
These are put back onto the model queue too.

Serving Layer
-------------

The serving layer listens for model and model updates on the model queue. It maintains model
state in memory. It exposes an HTTP 
[REST](http://en.wikipedia.org/wiki/Representational_state_transfer) API on top of methods 
that query the model in memory. There will be many of these deployed for scale. Each may 
also accept new data and write it to Kafka where it can be seen by all Speed layers.
The serving layer defaults are provided by a reference.conf. Applications are expected to provide their 
own conf file to provide properties which determine what application resources are required. This can be provided 
using an application.conf available in the classpath (which typesafe's Config will load automatically) or 
by providing a command line argument which specifies what package to look update application resources eg: -Dserving.application-resources=com.cloudera.oryx.ml.serving.als.

Usage and Deployment
--------------------

The application will be written in Java, using Spark 1.x, 
[Hadoop](http://hadoop.apache.org/) 2.4.x+, [Tomcat](http://tomcat.apache.org/) 8.x, 
Kafka 0.8.x, [Zookeeper](http://zookeeper.apache.org/), etc. Configuration will again 
use a single [Typesafe Config](https://github.com/typesafehub/config) config file, wherein 
applications configure an entire deployment of the system. This includes implementations of 
key interface classes which implement the batch, speed, and serving logic. Applications will 
package and deploy their implementations with each instance of the layer binaries. Each 
of these will be a runnable Java binary which starts all necessary services.


ML Tier Implementation
----------------------

The ML tier is simply an implementation and specialization of the generic interfaces mentioned 
above, which implement common ML needs and then expose a different ML-specific interface for
applications to fill in.

For example, it implements a batch layer update process that selects a test and training set
automatically. It calls an app-supplied function to evaluate the model on the test set.
It can automatically repeat this, and with different hyperparameter values, choosing the best
result. It manages serialization of the model via 
[PMML](http://www.dmg.org/v4-2-1/GeneralStructure.html).

End-to-end ML Application Implementation
----------------------------------------

Implementations of a complete recommender system based on ALS will be added, for example, as before.
This implementation will consist of the core model build in Spark, an in-memory
model update function adapted from existing code, and a port of REST APIs from the existing 
implementation.

Similarly for k-means++ and random decision forests, as before.


Contributors
------------

- Sean Owen (Cloudera)
- Suneel Marthi and Kam Kasravi (Intel)
