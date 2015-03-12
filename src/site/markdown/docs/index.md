# Documentation

* [JavaDoc and Other Project Reports](../project-reports.html)
* [API Endpoint Reference](api-end-pt-ref.html)
* [FAQ and Trouble Shooting](faq-and-troubleshooting.html)
* [Differences From Oryx 1](oryx-1-diff.html)
* [Oryx 2 First Release](oryx-2-first-release.html)
* [Wiki](https://github.com/cloudera/oryx/wiki)

# Build From Source
## Requirements

Building from source requires:

* [`git`](http://git-scm.com/), or an IDE that supports Git
* [Apache Maven](http://maven.apache.org/) 3.0.0 or later
* [Java JDK](http://www.oracle.com/technetwork/java/javase/downloads/index.html) (not just JRE) 6 or later

Some or all of these may already be installed on your development machine.

## Build

Clone the repository from GitHub in your desired local directory, which will create `oryx`. Build it:

```bash
git clone https://github.com/cloudera/oryx.git
cd oryx
mvn -DskipTests install
```

This will build the following binaries:

* Serving Layer: `serving/target/oryx-serving-x.y.z.jar`
* Computation Layer: `computation/target/oryx-computation-x.y.z.jar`

# Developing from Source

Note that if you are interested in developing on Oryx, you should probably [fork this repository](https://help.github.com/articles/fork-a-repo) and then work on your own fork, so that you can submit pull requests with changes.

## Older Hadoop Version (< 2.3.0) Note

To use Oryx with versions of Hadoop 2.x prior to 2.3.0, it is necessary to create compatible binaries by recompiling against the specific version of Hadoop you're using. To do so, use the `hadoop200` profile and set the `hadoop.version` property.

`mvn -phadoop200 -Dhadoop.version=...`

# Cluster Setup
## Requirements

- Java 7 or later (JRE only is required) (_In the near future, Java 8 may be required_)
- A Hadoop cluster running the following components:
    - Apache Hadoop 2.5.0 or later
    - Apache Zookeeper 3.4.5 or later
    - Apache Kafka 0.8.2 or later
    - Apache Spark 1.3.0 or later

[CDH](http://www.cloudera.com/content/cloudera/en/products-and-services/cdh.html)
5.4.0 and later meet these requirements, although any Hadoop distribution with these
components should work fine. While the rest of the instructions will refer to a CDH 5.4.0+
distribution, this is not a requirement.

_Note that the "alpha 1" release requires only Spark 1.2.0, and thus works with CDH 5.3.0+_

A single-node cluster can be sufficient, although running all of these components on one machine
may require a reasonable amount of RAM.

## Cluster Setup

Install and configure the Hadoop cluster normally. The following services need to be enabled:

- HDFS
- YARN
- Zookeeper
- Kafka
- Spark (on YARN)

Note that for CDH, Kafka is available as a "CLABS 1.0.0" parcel from
[Cloudera Labs](http://www.cloudera.com/content/cloudera/en/developers/home/cloudera-labs/apache-kafka.html).

Determine the (possibly several) Kafka brokers that are configured in the cluster, under Instances,
and note their hosts and port. The port is typically 9092. Same for the Zookeeper servers; the default
port here is 2181. Default ports will be used in subsequent examples.

Where a Kafka broker or Zookeeper server is called for, you can and should specify a comma-separated
list of `host:port` pairs where there are multiple hosts. Example: `your-zk-1:2181,your-zk-2:2181`.

Also note whether your Zookeeper instance is using a chroot path. This is simply a path suffixed
to the `host:port`, like `your-zk:2181/your-chroot`.
For example in CDH, Kafka uses a `/kafka` chroot, and subsequent examples will
use this chroot. You can omit this if you are not using a chroot.

Note: if you have multiple Zookeeper servers, and a chroot, only add the chroot once, at
the end: `your-zk-1:2181,your-zk-2:2181/kafka`

## Verifying Kafka (Optional)

To quickly verify that Kafka and ZK are running correctly:

```bash
kafka-topics --create --zookeeper your-zk:2181/kafka \
  --replication-factor 1 --partitions 1 --topic test
kafka-console-consumer --zookeeper your-zk:2181/kafka \
  --topic test --from-beginning
```

In another console, take any text file (here `data.csv`) and send it to the topic:

```bash
cat data.csv | kafka-console-producer \
  --broker-list your-kafka-broker:9092 --topic test
```

You should see the contents of the text file echoed onto the other consumer's console soon thereafter.

Delete the test topic when done.

```bash
kafka-topics --delete --zookeeper your-zk:2181/kafka --topic test
```

## Configuring Kafka


Oryx will use two Kafka topics for data transport. One carries input data to the batch and
Speed Layer, and the other carries model updates from there on to the Serving Layer. The default
names of these topics are "OryxInput" and "OryxUpdate" respectively. They need to be
created before Oryx is started.

Each can default to have one partition, but more can be configured if much higher read
throughput is needed.
The example below shows 1 partition. Replication factor can be any value, but 3 is recommended.

```bash
kafka-topics --create --zookeeper your-zk:2181/kafka \
  --replication-factor 3 --partitions 1 --topic OryxInput
...
Created topic "OryxInput".
```

```bash
kafka-topics --create --zookeeper your-zk:2181/kafka \
  --replication-factor 3 --partitions 1 --topic OryxUpdate
...
Created topic "OryxUpdate".
```

You may need to configure the retention time for one or both topics. In particular,
it's typically important to limit the retention time for the update topic, since the Speed
and Serving Layer read the entire topic from the start on startup to catch up. Setting it
to twice the Batch Layer update interval is a good start. For example, to set it to 2 days
(2 * 24 * 60 * 60 * 1000 = 172800000 ms):

```bash
kafka-topics --zookeeper your-zk:2181/kafka --alter --topic OryxUpdate \
  --config retention.ms=172800000
```

This is not as important for the input topic, which is not re-read from the beginning.

Continue to [[Running-Oryx]] to start the servers, and run an example.

# Configuration
Refer to the default configuration file for a list and explanation of configuration parameters:
[`reference.conf`](/OryxProject/oryx/blob/master/framework/oryx-common/src/main/resources/reference.conf)

Skeleton examples may be found at:

- [`app/conf/als-example.conf`](/OryxProject/oryx/blob/master/app/conf/als-example.conf)
- [`app/conf/kmeans-example.conf`](/OryxProject/oryx/blob/master/app/conf/kmeans-example.conf)
- [`app/conf/rdf-example.conf`](/OryxProject/oryx/blob/master/app/conf/rdf-example.conf)

# Running Oryx
_This is a temporary, manual process for distributing and running the binaries._

## Running

Download the [latest release](https://github.com/OryxProject/oryx/releases) of the Oryx Batch, Speed and Serving Layer, both `.jar` files and `.sh` scripts. Alternatively, build them from source (see [[Building-from-Source]]).

Copy binaries and scripts to machines that are part of the Hadoop cluster.
They may be deployed on different machines, or on one for purposes of testing.
The Speed and Batch Layers should run on at most one machine, each. The Serving Layer
can run on many.

Create a configuration file for your application. You may start with the example in
[conf/als-example.conf](/OryxProject/oryx/blob/master/app/conf/als-example.conf). Modify
host names, ports and directories. In particular, choose data and model directories on HDFS
that exist and will be accessible to the user running Oryx binaries.

Copy this config file as `example.conf` to the same directory as binaries and script
on each machine.

Run the three Layers with:

```bash
./run.sh --layer-jar oryx-batch-2.0.0-SNAPSHOT.jar --conf example.conf
...
./run.sh --layer-jar oryx-speed-2.0.0-SNAPSHOT.jar --conf example.conf
...
./run.sh --layer-jar oryx-serving-2.0.0-SNAPSHOT.jar --conf example.conf
```

These need not be on the same machine, but may be (if configuration specifies different
ports for the Batch and Speed Layer Spark web UI, and the Serving Layer API port).
The Serving Layer may be run on several machines.

That's all!

## Trying the ALS Example

If you've used the configuration above, you are running an instance of the ALS-based
recommender application.

Obtain the [http://grouplens.org/datasets/movielens/](GroupLens 100k) data set and find the
`u.data` file within. This needs to be converted to csv:

```bash
tr '\t' ',' < u.data > data.csv
```

You may wish to monitor the content of the input and update topic while it is in action.
[[Cluster-Setup]] explains how to tail topics with `kafka-console-consumer`. The topics are
named `OryxInput` and `OryxUpdate` by default.

Push the input to a Serving Layer, with a local command line tool like `curl`:

```bash
wget --post-file data.csv \
  --output-document - \
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
wget --quiet --output-document - \
  --server-response \
  http://your-serving-layer:8080/ready
...
  HTTP/1.1 200 OK
  Content-Length: 0
  Date: Thu, 18 Dec 2014 13:26:53 GMT
  Server: Oryx
```

```bash
wget --quiet --output-document - \
  http://your-serving-layer:8080/recommend/17
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

Congratulations, it's a live recommender!

When done, all processes can be killed with Ctrl-C safely.

See more about endpoints that are available in [[API-Endpoint-Reference]].

# Making an Oryx App
Oryx comes with an "app tier", implementations of actual Batch, Speed and Serving Layer
logic for recommendation, clustering and classification. However, any implementation
may be used with Oryx. They can be mixed and matched too. For example, you could reimplement
the Batch Layer for ALS-related recommendation and instead supply this alternative
implementation while still using the provided ALS Serving and Speed Layers.

## Creating an App

In each case, creating a custom Batch, Speed or Serving Layer app amounts to implementing
one Java interface or Scala trait. These interfaces/traits are found in the `oryx-api` module
within the project.

|         | Java                                                |
| -------:|:--------------------------------------------------- |
| Batch   | `com.cloudera.oryx.api.batch.BatchLayerUpdate`      |
| Speed   | `com.cloudera.oryx.api.speed.SpeedModelManager`     |
| Serving | `com.cloudera.oryx.api.serving.ServingModelManager` |

|         | Scala                                                    |
| -------:|:-------------------------------------------------------- |
| Batch   | `com.cloudera.oryx.api.batch.ScalaBatchLayerUpdate`      |
| Speed   | `com.cloudera.oryx.api.speed.ScalaSpeedModelManager`     |
| Serving | `com.cloudera.oryx.api.serving.ScalaServingModelManager` |

## Building an App

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

A minimal skeleton project can be found at [example/](/OryxProject/oryx/tree/master/app/example).

Compile your code and create a JAR file containing only your implementation, and any supporting
third-party code. With Maven, this happens with `mvn package`.

## Customizing an Oryx App

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

## Deploying an App

Copy the resulting JAR file -- call it `myapp.jar` -- to the directory containing the
Oryx binary JAR file it will be run with.

Change your Oryx `.conf` file to refer to your custom Batch, Speed or Serving implementation
class, as appropriate.

When running the Batch / Speed / Serving Layers, add `--app-jar myapp.jar` to the `run.sh`
command line.

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

