title: Docs: Dev

# Building from Source

## Requirements

- [`git`](http://git-scm.com/), or an IDE that supports Git
- [Apache Maven](http://maven.apache.org/) 3.2.1 or later
- [Java JDK](http://www.oracle.com/technetwork/java/javase/downloads/index.html) (not just JRE) 8 or later

Some or all of these may already be installed on your development machine.

## Building

Clone the repository in your desired local directory and build it:

```bash
git clone https://github.com/OryxProject/oryx.git oryx
cd oryx
mvn -DskipTests package
```

This will build the following binaries:

- Batch Layer: `deploy/oryx-batch/target/oryx-batch-2.3.0.jar`
- Speed Layer: `deploy/oryx-speed/target/oryx-speed-2.3.0.jar`
- Serving Layer: `deploy/oryx-serving/target/oryx-serving-2.3.0.jar`

Note that if you are interested in developing on Oryx, you should probably 
[fork this repository](https://help.github.com/articles/fork-a-repo) and then work on 
your own fork, so that you can submit pull requests with changes.

### Platform Only

The default build includes end-to-end ML applications based on Spark MLlib and other
libraries. To build only the lambda tier and ML tier, for use with your own app, disable
the `app-tier` profile: `-P!app-tier`. Note that in `bash`, `!` is reserved, so you may
need to add `-P\!app-tier`.

## Testing

`mvn test` runs all unit tests. `mvn verify` will run all integration tests too, which takes
significantly longer.

# Module Mapping

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

# Making an Oryx App

Oryx comes with an "app tier", implementations of actual Batch, Speed and Serving Layer
logic for recommendation, clustering and classification. However, any implementation
may be used with Oryx. They can be mixed and matched too. For example, you could reimplement
the Batch Layer for ALS-related recommendation and instead supply this alternative
implementation while still using the provided ALS Serving and Speed Layers.

## Creating an App

In each case, creating a custom Batch, Speed or Serving Layer app amounts to implementing
a few key Java interfaces or Scala traits in `com.cloudera.oryx.api`. 
These interfaces/traits are found in the `oryx-api` module within the project.

|         | Java    | Scala   |
| -------:|:------- |:------- |
| Batch   | [`batch.BatchLayerUpdate`](https://github.com/OryxProject/oryx/blob/master/framework/oryx-api/src/main/java/com/cloudera/oryx/api/batch/BatchLayerUpdate.java) | [`batch.ScalaBatchLayerUpdate`](https://github.com/OryxProject/oryx/blob/master/framework/oryx-lambda/src/main/scala/com/cloudera/oryx/lambda/batch/ScalaBatchLayerUpdateAdapter.scala) |
| Speed   | [`speed.SpeedModelManager`](https://github.com/OryxProject/oryx/blob/master/framework/oryx-api/src/main/java/com/cloudera/oryx/api/speed/SpeedModelManager.java) | [`speed.ScalaSpeedModelManager`](https://github.com/OryxProject/oryx/blob/master/framework/oryx-api/src/main/scala/com/cloudera/oryx/api/speed/ScalaSpeedModelManager.scala) |
| Serving | [`serving.ServingModelManager`](https://github.com/OryxProject/oryx/blob/master/framework/oryx-api/src/main/java/com/cloudera/oryx/api/serving/ServingModelManager.java) | [`serving.ScalaServingModelManager`](https://github.com/OryxProject/oryx/blob/master/framework/oryx-api/src/main/scala/com/cloudera/oryx/api/serving/ScalaServingModelManager.scala) |

`com.cloudera.oryx.api` also contains key support classes and interfaces used by these interfaces.
For example, [`serving.OryxResource`](https://github.com/OryxProject/oryx/blob/master/framework/oryx-api/src/main/java/com/cloudera/oryx/api/serving/OryxResource.java)
is a starting point for building custom JAX-RS endpoints, but need not be used.

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
    <version>2.3.0</version>
  </dependency>
</dependencies>
```

A minimal skeleton project can be found at [example/](https://github.com/OryxProject/oryx/tree/master/app/example).
In the spirit of "word count", this application consumes lines of space-separated words, and counts the number of
distinct words that each word occurs with in a line. For example, in "the quicker the better", each word occurs
with 2 other distinct words.

Compile your code and create a JAR file containing only your implementation, and any supporting
third-party code. With Maven, this happens with `mvn package`.

Note: to enable native BLAS acceleration in your app, see additional notes about BLAS in the
[performance](performance.html) documentation.

### Compiling the Word Count Example

Building the entire project with `mvn ... package` per above will actually build the example. 
The application JAR is produced at `target/example-2.3.0.jar` for example.

To rebuild or repackage just the word count example:

```
cd app/example
mvn package
```

Note that this won't work when building from the head of a branch (a `SNAPSHOT` version) unless
you first `mvn ... install` the project artifacts locally.

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
    <version>2.3.0</version>
  </dependency>
</dependencies>
```

## Deploying an App

Copy the resulting JAR file -- call it `myapp.jar` -- to the directory containing the
Oryx binary JAR file it will be run with.

Change your Oryx `.conf` file to refer to your custom Batch, Speed or Serving implementation
class, as appropriate.

When running the Batch / Speed / Serving Layers, add `--app-jar myapp.jar` to the `oryx-run.sh`
command line.

### Deploying the Word Count Example

For example, if you've built the packaged example "word count" app above, you can run it by
copying and adapting the provided [`wordcount-example.conf`](https://github.com/OryxProject/oryx/tree/master/app/conf/wordcount-example.conf)
configuration file:

```
./oryx-run.sh batch --conf wordcount-example.conf --app-jar example-2.3.0.jar
```

... and similarly for the speed and serving layers. Feed lines of input and then observe counts:

```
curl -X POST http://.../add/foo%20bar%20baz
...
curl http://.../distinct
{"foo":2,"bar":2,"baz":2}
```

The configuration file itself is already configured to match the hostname and general parameters of the
[Cloudera Quickstart VM](http://www.cloudera.com/content/www/en-us/downloads/quickstart_vms.html). In fact,
this example should work as-is on a demo cluster set up according to the 
[Cloudera Quickstart VM Setup](admin.html#cloudera_quickstart_vm_setup).
