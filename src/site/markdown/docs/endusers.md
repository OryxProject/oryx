title: Docs: End Users

# Running

## Setup

You must have set up and configured your cluster as shown in the [Admin docs](admin.html).

In particular, you should have already installed Java 8 on your cluster. In order to make
subsequent `oryx-run.sh` commands work, it is likely necessary to update the default Java 
version with `update-alternatives --config java` or equivalent to select Java 8,
and set `JAVA_HOME` to point to the Java 8 installation.

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

## Execute

Run the three Layers with:

```bash
./oryx-run.sh batch
./oryx-run.sh speed
./oryx-run.sh serving
```

`--layer-jar your-layer.jar` and `--conf your-config.conf` can be used to specify
an alternative location of the layer `.jar` and/or `.conf` file. You can use `--jvm-args`
to pass more arguments directly the Spark driver program, like system properties: `--jvm-args="-Dkey=value"`

These need not be on the same machine, but may be (if configuration specifies different
ports for the Batch and Speed Layer Spark web UI, and the Serving Layer API port).
The Serving Layer may be run on several machines.

You can see, for example, the Batch Layer Spark UI running on port 4040 of the machine
on which you started it (unless your configuration changed this). A simple web-based
console for the Serving Layer is likewise available on port 8080 by default.

That's all!

## Trying the ALS Example

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
`model.pmml` files are PMML files containing elements like:

```
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<PMML xmlns="http://www.dmg.org/PMML-4_3" version="4.3">
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
    <Extension name="implicit" value="false"/>
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
  Date: Tue, 1 Sep 2015 13:26:53 GMT
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

# API Endpoint Reference

Oryx bundles several end-to-end applications, including a Serving Layer with REST endpoints.

## Collaborative filtering / Recommendation

* [`/recommend`](http://oryx.io/apidocs/com/cloudera/oryx/app/serving/als/Recommend.html)
* [`/recommendToMany`](http://oryx.io/apidocs/com/cloudera/oryx/app/serving/als/RecommendToMany.html)
* [`/recommendToAnonymous`](http://oryx.io/apidocs/com/cloudera/oryx/app/serving/als/RecommendToAnonymous.html)
* [`/recommendWithContext`](http://oryx.io/apidocs/com/cloudera/oryx/app/serving/als/RecommendWithContext.html)
* [`/similarity`](http://oryx.io/apidocs/com/cloudera/oryx/app/serving/als/Similarity.html)
* [`/similarityToItem`](http://oryx.io/apidocs/com/cloudera/oryx/app/serving/als/SimilarityToItem.html)
* [`/knownItems`](http://oryx.io/apidocs/com/cloudera/oryx/app/serving/als/KnownItems.html)
* [`/estimate`](http://oryx.io/apidocs/com/cloudera/oryx/app/serving/als/Estimate.html)
* [`/estimateForAnonymous`](http://oryx.io/apidocs/com/cloudera/oryx/app/serving/als/EstimateForAnonymous.html)
* [`/because`](http://oryx.io/apidocs/com/cloudera/oryx/app/serving/als/Because.html)
* [`/mostSurprising`](http://oryx.io/apidocs/com/cloudera/oryx/app/serving/als/MostSurprising.html)
* [`/popularRepresentativeItems`](http://oryx.io/apidocs/com/cloudera/oryx/app/serving/als/PopularRepresentativeItems.html)
* [`/mostActiveUsers`] (http://oryx.io/apidocs/com/cloudera/oryx/app/serving/als/MostActiveUsers.html)
* [`/mostPopularItems`](http://oryx.io/apidocs/com/cloudera/oryx/app/serving/als/MostPopularItems.html)
* [`/mostActiveUsers`] (http://oryx.io/apidocs/com/cloudera/oryx/app/serving/als/MostActiveUsers.html)
* [`/item/allIDs`](http://oryx.io/apidocs/com/cloudera/oryx/app/serving/als/AllItemIDs.html)
* [`/ready`](http://oryx.io/apidocs/com/cloudera/oryx/app/serving/Ready.html)
* [`/pref`](http://oryx.io/apidocs/com/cloudera/oryx/app/serving/als/Preference.html)
* [`/ingest`](http://oryx.io/apidocs/com/cloudera/oryx/app/serving/als/Ingest.html)

## Classification / Regression

* [`/predict`](http://oryx.io/apidocs/com/cloudera/oryx/app/serving/rdf/Predict.html)
* [`/classificationDistribution`](http://oryx.io/apidocs/com/cloudera/oryx/app/serving/rdf/ClassificationDistribution.html)
* [`/ready`](http://oryx.io/apidocs/com/cloudera/oryx/app/serving/Ready.html)
* [`/train`](http://oryx.io/apidocs/com/cloudera/oryx/app/serving/rdf/Train.html)

## Clustering

* [`/assign`](http://oryx.io/apidocs/com/cloudera/oryx/app/serving/kmeans/Assign.html)
* [`/distanceToNearest`](http://oryx.io/apidocs/com/cloudera/oryx/app/serving/kmeans/DistanceToNearest.html)
* [`/ready`](http://oryx.io/apidocs/com/cloudera/oryx/app/serving/Ready.html)
* [`/add`](http://oryx.io/apidocs/com/cloudera/oryx/app/serving/kmeans/Add.html)

# Configuration

Refer to the default configuration file for a list and explanation of configuration parameters:
[`reference.conf`](https://github.com/OryxProject/oryx/blob/master/framework/oryx-common/src/main/resources/reference.conf)

Or see one of the following examples:

- [`app/conf/als-example.conf`](https://github.com/OryxProject/oryx/blob/master/app/conf/als-example.conf)
- [`app/conf/kmeans-example.conf`](https://github.com/OryxProject/oryx/blob/master/app/conf/kmeans-example.conf)
- [`app/conf/rdf-classification-example.conf`](https://github.com/OryxProject/oryx/blob/master/app/conf/rdf-classification-example.conf)
- [`app/conf/rdf-regression-example.conf`](https://github.com/OryxProject/oryx/blob/master/app/conf/rdf-regression-example.conf)
