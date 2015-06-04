# End Users

## Running

*Note: You must have set up and configured your cluster as shown in the Administrator section below.*

Download the [latest release](https://github.com/OryxProject/oryx/releases) of the Oryx Batch, 
Speed and Serving Layer, both `.jar` files and `.sh` scripts. Alternatively, build them 
from source and obtain the latest scripts from 
[deploy/bin/](https://github.com/OryxProject/oryx/blob/master/deploy/bin).

Copy binaries and scripts to machines that are part of the Hadoop cluster.
They may be deployed on different machines, or on one for purposes of testing.
The Speed and Batch Layers should run on at most one machine, each. The Serving Layer
can run on many.

Create a configuration file for your application. You may start with the example in
[conf/als-example.conf](https://github.com/OryxProject/oryx/blob/master/app/conf/als-example.conf). Modify
host names, ports and directories. In particular, choose data and model directories on HDFS
that exist and will be accessible to the user running Oryx binaries.

Copy this config file as `oryx.conf` to the same directory as binaries and script
on each machine.

Run the three Layers with:

```bash
./oryx-run.sh batch
./oryx-run.sh speed
./oryx-run.sh serving
```

(`--layer-jar your-layer.jar` and `--conf your-config.conf` can be used to specify
an alternative location of the layer `.jar` and/or `.conf` file.)

These need not be on the same machine, but may be (if configuration specifies different
ports for the Batch and Speed Layer Spark web UI, and the Serving Layer API port).
The Serving Layer may be run on several machines.

You can see, for example, the Batch Layer Spark UI running on port 4040 of the machine
on which you started it (unless your configuration changed this). A simple web-based
console for the Serving Layer is likewise available on port 8080 by default.

That's all!

### Trying the ALS Example

If you've used the configuration above, you are running an instance of the ALS-based
recommender application.

Obtain the [GroupLens 100K](http://grouplens.org/datasets/movielens/) data set and find the
`u.data` file within. This needs to be converted to csv:

```bash
tr '\t' ',' < u.data > data.csv
```

Push the input to a Serving Layer, with a local command line tool like `curl`:

```bash
wget --quiet --post-file data.csv --output-document - \
  --header "Content-Type: text/csv" \
  http://your-serving-layer:8080/ingest
```

If you are tailing the input topic, you should see a large amount of CSV data flow to the topic:

```
196,242,3.0,881250949186
196,242,3.0,881250949
186,302,3.0,891717742
22,377,1.0,878887116
244,51,2.0,880606923
166,346,1.0,886397596
298,474,4.0,884182806
...
```

Soon, you should also see the Batch Layer trigger a new computation. The example configuration
starts one every 5 minutes.

The data is first written to HDFS. The example configuration has
it written to directories under `hdfs:///user/example/Oryx/data/`. Within are directories
named by timestamp, each containing Hadoop `part-r-*` files, which contain the input as
`SequenceFile`s of `Text`. Although not pure text, printing them should yield some recognizable
data because it is in fact text.

```
SEQorg.apache.hadoop.io.Textorg.apache.hadoop.io.Text����^�]�XسN�22,377,1.0,87888711662...
```

A model computation then begins. This should show as a number of new distributed jobs the
Batch Layer. Its Spark UI is started at `http://your-batch-layer:4040` in the example
configuration.

Soon the model will complete, and it will be persisted as a combination of PMML and supporting
data files in a subdirectory of `hdfs:///user/example/Oryx/model/`. For example, the
`model.pmml.gz` files are compressed PMML files containing elements like:

```
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<PMML xmlns="http://www.dmg.org/PMML-4_2" version="4.2.1">
    <Header>
        <Application name="Oryx"/>
        <Timestamp>2014-12-18T04:48:54-0800</Timestamp>
    </Header>
    <Extension name="X" value="X/"/>
    <Extension name="Y" value="Y/"/>
    <Extension name="features" value="10"/>
    <Extension name="lambda" value="0.001"/>
    <Extension name="implicit" value="true"/>
    <Extension name="alpha" value="1.0"/>
    <Extension name="XIDs">56 168 222 343 397 ...
     ...
```

The `X/` and `Y/` subdirectories next to it contain feature vectors, like:

```
[56,[0.5746282834154238,-0.08896614131333057,-0.029456222765775263,
  0.6039821219690552,0.1497901814774658,-0.018654312114339863,
  -0.37342063488340266,-0.2370768843521807,1.148260034028485,
  1.0645643656769153]]
[168,[0.8722769882777296,0.4370416943031704,0.27402044461549885,
  -0.031252701117490456,-0.7241385753098256,0.026079081002582338,
  0.42050973702065714,0.27766923396205817,0.6241033215856671,
  -0.48530795198811266]]
...
```

If you are tailing the update topic, you should also see these values published to the
topic.

The Serving Layer will pick this up soon thereafter, and the `/ready` endpoint will return
status `200 OK`:

```bash
wget --quiet --output-document - --server-response \
  http://your-serving-layer:8080/ready
...
  HTTP/1.1 200 OK
  Content-Length: 0
  Date: Thu, 18 Dec 2014 13:26:53 GMT
  Server: Oryx
```

```bash
wget --quiet --output-document -  http://your-serving-layer:8080/recommend/17
...
50,0.7749542842056966
275,0.7373013861581563
258,0.731818692628511
181,0.7049967175706345
127,0.704518989947498
121,0.7014631029793741
15,0.6954683387287907
288,0.6774889711024022
25,0.6663619887033064
285,0.6398968471343595
```

Congratulations, it's a live recommender! When done, all processes can be killed with Ctrl-C safely.

## API Endpoint Reference

Oryx bundles several end-to-end applications, including a Serving Layer with REST endpoints.

### Collaborative filtering / Recommendation

* [`/recommend`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/Recommend.html)
* [`/recommendToMany`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/RecommendToMany.html)
* [`/recommendToAnonymous`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/RecommendToAnonymous.html)
* [`/similarity`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/Similarity.html)
* [`/similarityToItem`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/SimilarityToItem.html)
* [`/knownItems`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/KnownItems.html)
* [`/estimate`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/Estimate.html)
* [`/estimateForAnonymous`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/EstimateForAnonymous.html)
* [`/because`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/Because.html)
* [`/mostSurprising`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/MostSurprising.html)
* [`/popularRepresentativeItems`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/PopularRepresentativeItems.html)
* [`/mostActiveUsers`] (http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/MostActiveUsers.html)
* [`/mostPopularItems`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/MostPopularItems.html)
* [`/mostActiveUsers`] (http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/MostActiveUsers.html)
* [`/item/allIDs`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/AllItemIDs.html)
* [`/ready`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/Ready.html)
* [`/pref`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/Preference.html)
* [`/ingest`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/als/Ingest.html)

### Classification / Regression

* [`/predict`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/rdf/Predict.html)
* [`/classificationDistribution`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/rdf/ClassificationDistribution.html)
* [`/train`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/rdf/Train.html)

### Clustering

* [`/assign`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/kmeans/Assign.html)
* [`/distanceToNearest`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/kmeans/DistanceToNearest.html)
* [`/add`](http://oryxproject.github.io/oryx/apidocs/com/cloudera/oryx/app/serving/kmeans/Add.html)

## Configuration

Refer to the default configuration file for a list and explanation of configuration parameters:
[`reference.conf`](https://github.com/OryxProject/oryx/blob/master/framework/oryx-common/src/main/resources/reference.conf)

Or see one of the following examples:

- [`app/conf/als-example.conf`](https://github.com/OryxProject/oryx/blob/master/app/conf/als-example.conf)
- [`app/conf/kmeans-example.conf`](https://github.com/OryxProject/oryx/blob/master/app/conf/kmeans-example.conf)
- [`app/conf/rdf-example.conf`](https://github.com/OryxProject/oryx/blob/master/app/conf/rdf-example.conf)


# Developers

## Building from Source

### Requirements

- [`git`](http://git-scm.com/), or an IDE that supports Git
- [Apache Maven](http://maven.apache.org/) 3.2.1 or later
- [Java JDK](http://www.oracle.com/technetwork/java/javase/downloads/index.html) (not just JRE) 7 or later

Some or all of these may already be installed on your development machine.

### Building

Clone the repository in your desired local directory and build it:

```bash
git clone https://github.com/OryxProject/oryx.git oryx
cd oryx
mvn -DskipTests package
```

This will build the following binaries:

- Batch Layer: `deploy/oryx-batch/target/oryx-batch-x.y.z.jar`
- Speed Layer: `deploy/oryx-speed/target/oryx-speed-x.y.z.jar`
- Serving Layer: `deploy/oryx-serving/target/oryx-serving-x.y.z.jar`

... where `x.y.z` is the current version string, like `2.0.0`.

Note that if you are interested in developing on Oryx, you should probably 
[fork this repository](https://help.github.com/articles/fork-a-repo) and then work on 
your own fork, so that you can submit pull requests with changes.

#### Java 8

To configure the build to use Java 8, add `-Pjava8` to build and test commands here.

#### Platform Only

The default build includes end-to-end ML applications based on Spark MLlib and other
libraries. To build only the lambda tier and ML tier, for use with your own app, disable
the `app-tier` profile: `-P!app-tier`. Note that in `bash`, `!` is reserved, so you may
need to add `-P\!app-tier`.

### Testing

`mvn test` runs all unit tests. `mvn verify` will run all integration tests too, which takes
significantly longer.

## Module Mapping

Major modules and their relation to tiers and layers:

|          | *Serving*             | *Speed*                     | *Batch*                     |
| --------:| --------------------- | --------------------------- | --------------------------- |
| *Binary* | [`oryx-serving`](https://github.com/OryxProject/oryx/tree/master/deploy/oryx-serving) | [`oryx-speed`](https://github.com/OryxProject/oryx/tree/master/deploy/oryx-speed) | [`oryx-batch`](https://github.com/OryxProject/oryx/tree/master/deploy/oryx-batch) |
| *App*    | [`oryx-app-serving`](https://github.com/OryxProject/oryx/tree/master/app/oryx-app-serving) | [`oryx-app-mllib`](https://github.com/OryxProject/oryx/tree/master/app/oryx-app-mllib) [`oryx-app`](https://github.com/OryxProject/oryx/tree/master/app/oryx-app) | [`oryx-app-mllib`](https://github.com/OryxProject/oryx/tree/master/app/oryx-app-mllib) [`oryx-app`](https://github.com/OryxProject/oryx/tree/master/app/oryx-app) |
| *ML*     |                       | [`oryx-ml`](https://github.com/OryxProject/oryx/tree/master/framework/oryx-ml) | [`oryx-ml`](https://github.com/OryxProject/oryx/tree/master/framework/oryx-ml) |
| *Lambda* | [`oryx-lambda-serving`](https://github.com/OryxProject/oryx/tree/master/framework/oryx-lambda-serving) | [`oryx-lambda`](https://github.com/OryxProject/oryx/tree/master/framework/oryx-lambda) | [`oryx-lambda`](https://github.com/OryxProject/oryx/tree/master/framework/oryx-lambda) |

Supporting modules like 
[`oryx-common`](https://github.com/OryxProject/oryx/tree/master/framework/oryx-common), 
[`oryx-app-common`](https://github.com/OryxProject/oryx/tree/master/app/oryx-app-common),
[`oryx-api`](https://github.com/OryxProject/oryx/tree/master/framework/oryx-api), and
[`oryx-app-api`](https://github.com/OryxProject/oryx/tree/master/app/oryx-app-api) are not shown.

## Making an Oryx App

Oryx comes with an "app tier", implementations of actual Batch, Speed and Serving Layer
logic for recommendation, clustering and classification. However, any implementation
may be used with Oryx. They can be mixed and matched too. For example, you could reimplement
the Batch Layer for ALS-related recommendation and instead supply this alternative
implementation while still using the provided ALS Serving and Speed Layers.

### Creating an App

In each case, creating a custom Batch, Speed or Serving Layer app amounts to implementing
one Java interface or Scala trait. These interfaces/traits are found in the `oryx-api` module
within the project.

|         | Java                                                |
| -------:|:--------------------------------------------------- |
| Batch   | [`com.cloudera.oryx.api.batch.BatchLayerUpdate`](https://github.com/OryxProject/oryx/blob/master/framework/oryx-api/src/main/java/com/cloudera/oryx/api/batch/BatchLayerUpdate.java)      |
| Speed   | [`com.cloudera.oryx.api.speed.SpeedModelManager`](https://github.com/OryxProject/oryx/blob/master/framework/oryx-api/src/main/java/com/cloudera/oryx/api/speed/SpeedModelManager.java)     |
| Serving | [`com.cloudera.oryx.api.serving.ServingModelManager`](https://github.com/OryxProject/oryx/blob/master/framework/oryx-api/src/main/java/com/cloudera/oryx/api/serving/ServingModelManager.java) |

|         | Scala                                                    |
| -------:|:-------------------------------------------------------- |
| Batch   | [`com.cloudera.oryx.api.batch.ScalaBatchLayerUpdate`](https://github.com/OryxProject/oryx/blob/master/framework/oryx-lambda/src/main/java/com/cloudera/oryx/lambda/batch/ScalaBatchLayerUpdateAdapter.java)      |
| Speed   | [`com.cloudera.oryx.api.speed.ScalaSpeedModelManager`](https://github.com/OryxProject/oryx/blob/master/framework/oryx-api/src/main/scala/com/cloudera/oryx/api/speed/ScalaSpeedModelManager.scala)     |
| Serving | [`com.cloudera.oryx.api.serving.ScalaServingModelManager`](https://github.com/OryxProject/oryx/blob/master/framework/oryx-api/src/main/scala/com/cloudera/oryx/api/serving/ScalaServingModelManager.scala) |

### Building an App

To access these interfaces/traits in your application, add a dependency on
`com.cloudera.oryx:oryx-api`. The scope should be `provided`.

In Maven, this would mean adding a dependency like:

```XML
<dependencies>
  <dependency>
    <groupId>com.cloudera.oryx</groupId>
    <artifactId>oryx-api</artifactId>
    <scope>provided</scope>
    <version>2.0.0</version>
  </dependency>
</dependencies>
```

The artifacts are currently hosted at the 
[Cloudera repository](https://repository.cloudera.com/artifactory/cloudera-repos/), so your build will also 
need a reference to this repo:

```XML
<repositories>
  <repository>
    <id>cloudera</id>
    <url>https://repository.cloudera.com/artifactory/cloudera-repos/</url>
  </repository>
</repositories>
```

A minimal skeleton project can be found at [example/](https://github.com/OryxProject/oryx/tree/master/app/example).

Compile your code and create a JAR file containing only your implementation, and any supporting
third-party code. With Maven, this happens with `mvn package`.

### Customizing an Oryx App

When deploying the prepackaged applications that come with Oryx, in some cases, it's possible
to supply additional implementations to customize their behavior. For example, the ALS recommender
application exposes a `com.cloudera.oryx.app.als.RescorerProvider` interface.
These app-specific API classes are found in module `oryx-app-api`. Implementations of
interfaces like these can be compiled, packaged and deployed in the same way described
here for stand-alone applications.

```XML
<dependencies>
  <dependency>
    <groupId>com.cloudera.oryx</groupId>
    <artifactId>oryx-app-api</artifactId>
    <scope>provided</scope>
    <version>2.0.0</version>
  </dependency>
</dependencies>
```

### Deploying an App

Copy the resulting JAR file -- call it `myapp.jar` -- to the directory containing the
Oryx binary JAR file it will be run with.

Change your Oryx `.conf` file to refer to your custom Batch, Speed or Serving implementation
class, as appropriate.

When running the Batch / Speed / Serving Layers, add `--app-jar myapp.jar` to the `oryx-run.sh`
command line.


# Administrators

## Cluster Setup

The following are required:

- Java 7 or later (JRE only is required)
- A Hadoop cluster running the following components:
    - Apache Hadoop 2.6.0 or later
    - Apache Zookeeper 3.4.5 or later
    - Apache Kafka 0.8.2 or later
    - Apache Spark 1.3.0 or later

[CDH](http://www.cloudera.com/content/cloudera/en/products-and-services/cdh.html)
5.4.0 and later meet these requirements, although any Hadoop distribution with these
components should work fine. While the rest of the instructions will refer to a CDH 5.4.0+
distribution, this is not a requirement.

A single-node cluster can be sufficient, although running all of these components on one machine
may require a reasonable amount of RAM.

### Services

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

### Configuring Kafka

Oryx will use two Kafka topics for data transport. One carries input data to the batch and
Speed Layer, and the other carries model updates from there on to the Serving Layer. The default
names of these topics are `OryxInput` and `OryxUpdate` respectively. They need to be
created before Oryx is started.

Each can default to have one partition, but more can be configured if much higher read
throughput is needed. Replication factor can be any value, but at least 2 is recommended.
Note that the replication factor can't exceed the number of Kafka brokers in the cluster.

You may need to configure the retention time for one or both topics. In particular,
it's typically important to limit the retention time for the update topic, since the Speed
and Serving Layer read the entire topic from the start on startup to catch up. 
This is not as important for the input topic, which is not re-read from the beginning.

Setting it
to twice the Batch Layer update interval is a good start. For example, to set it to 1 day
(24 * 60 * 60 * 1000 = 86400000 ms), set the topic's `retention.ms` property to 86400000.
This is done automatically by the provided `oryx-run.sh kafka-setup` script.

### Automated Configuration

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
Topic:OryxInput	PartitionCount:1	ReplicationFactor:2	Configs:
	Topic: OryxInput	Partition: 0	Leader: 120	Replicas: 120,121	Isr: 120,121

Update topic OryxUpdate does not exist. Create it? y
Creating topic OryxUpdate
Created topic "OryxUpdate".
Updated config for topic "OryxUpdate".
Status of topic OryxUpdate:
Topic:OryxUpdate	PartitionCount:1	ReplicationFactor:2	Configs:retention.ms=86400000
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

Then in another window, you can feed input, such as the `data.csv` example from above, 
into the input queue to verify it's working with:

```bash
./oryx-run.sh kafka-input --input-file data.csv
```

If all is well, these processes can be terminated. The cluster is ready to run Oryx.

## Handling Failure

Eventually, you'll want to stop one or more of the Layers running, or restart it. Or maybe
a server decides to die. What happens then? What's the worst that can happen?

### Data Loss

Historical data is saved in HDFS, which should be configured for replication. HDFS ensures
data is stored reliably. Kafka is also designed to cope with failure when configured to use
replication.

That is, there is nothing special to do here in order to ensure that data is
never completely lost. It is the job of HDFS and Kafka to always be available and not lose
data.

### Server Failure

In general, all three Layer server processes should run continuously, and can and should be
restarted immediately if they have to be stopped, or in case of a failure.
This can be accomplished with an init script or similar mechanism (not included, yet).

#### Serving Layer

The Serving Layer has no state. On startup, it reads all models and updates available on the
update topic. It begins answering queries as soon as any first, valid model is
available. For this reason, it's desirable to limit the retention time for the update topic.

The operation of the Serving Layer is not distributed. Each instance is independent, and may
stop or start without affecting others.

#### Speed Layer

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

#### Batch Layer

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

## Differences from Oryx 1

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

### Architecture Differences

| Oryx 1 | Oryx 2 |
| ------ | ------ |
| One monolithic "tier" for lambda architecture and apps | Three tiers: lambda,  ML, apps |
| No app-level extensibility | Platform for building other lambda- and ML-based apps |
| Two layers: Computation and Serving | Three layers: Batch, Speed and Serving |
| Based on Crunch, MapReduce, HDFS, Tomcat | Based on HDFS, YARN, Spark (+ Streaming, MLlib), Kafka, Zookeeper, Tomcat |
| 27K lines production code / 4K test | 10K lines production code / 7.5K test: simpler, better tested |

### Deployment Differences

| Oryx 1 | Oryx 2 |
| ------ | ------ |
| Requires Java 6, optionally core Hadoop 2.2+ (including "MR1") | Requires Java 7, core Hadoop 2.5+ (YARN, not "MR1") Spark 1.3+, Kafka 0.8.2+, Zookeeper 3.4.5+ |
| Supports local, non-Hadoop deployment  | No non-Hadoop deployment |
| Supports MapReduce-based Hadoop deployment | Supports  only deployment with core Hadoop, YARN, Spark, Kafka |

### Scale and Reliability Differences

| Oryx 1 | Oryx 2 |
| ------ | ------ |
| Memory-efficient | Fast, memory-hungry |
| Custom, best-effort data transport between layers | Reliable data transport via Kafka |
| Custom MapReduce-based algorithm implementations in Computation Layer | Spark Streaming-based batch layer framework and Spark MLlib-based algorithm implementations |
| Custom in-core incremental model update ("speed layer") | Spark Streaming-based distributed model update |

### Migration

The bad news is that no direct migration is possible between Oryx 1 and Oryx 2; they have 
very different implementations. However, differences in the user- and developer-facing aspects 
are by design similar or identical.

#### REST API

Oryx 2 contains the same set of end-to-end ML applications as Oryx 1, and exposes virtually 
the same REST API, unchanged. The only significant difference is that there is no longer 
a `/refresh` endpoint, because it is unnecessary.

#### Configuration

Both implementations use a single configuration file parsed by Typesafe Config.
The property namespaces are different but there are some similarities. Compare the 
[Oryx 1 configuration](https://github.com/cloudera/oryx/blob/master/common/src/main/resources/reference.conf) 
to the 
[Oryx 2 configuration](https://github.com/OryxProject/oryx/blob/master/framework/oryx-common/src/main/resources/reference.conf) 
to understand some of the correspondence and difference.

#### Data Storage and Transport

In Oryx 1, all data was stored in a series of directories in HDFS. In Oryx 2, data is transported 
via Kafka (which ultimately stores data in HDFS) and in HDFS as managed by a Spark Streaming 
process. Although it is still possible to side-load data files via HDFS in Oryx 2, it is not 
supported and is discouraged, in favor of sending data directly to a Kafka queue.

#### Data Formats

In theory, the framework is agnostic to data types and encodings passed between layers. 
In practice, the provided applications consume the same CSV-encoded data format as Oryx 1.

#### Deployment

The deployment requirements are the most different. Although all layers are still distributed 
as Java `.jar` binaries, now, a Hadoop cluster is required, including HDFS, YARN, Kafka, Spark, 
and Zookeeper services. Your environment or cluster must be updated to include these services 
before you can use Oryx 2.

## Troubleshooting

### Unsupported major.minor version 51.0

This means you are running Java 6 somewhere. Oryx 2 requires Java 7 or later.

### Initial job has not accepted any resources

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

### Required executor memory (... MB) is above the max threshold (... MB) of this cluster

This means your YARN configuration limits the maximum container size that can be requested.
Increase the Container Memory Maximum (`yarn.scheduler.maximum-allocation-mb`)
to something larger. For Spark, it generally makes sense to allow large containers.
