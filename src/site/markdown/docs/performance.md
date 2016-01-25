title: Performance

This collects assorted comments, rules of thumb, and benchmarks related to performance: how much 
resource various things take to do various amounts of work.

# Hardware and Cluster Design

In general there are no special hardware or cluster requirements. The cluster resource requirements will
be driven primarily by Spark-based jobs, which tend to be memory-intensive and occasionally CPU-intensive,
but not generally I/O-bound. If data ingest rate is *very* high, Kafka may need some special consideration.
In both cases, there is nothing different about sizing these from any other Kafka / Spark job.

# Data Transport

Since Kafka is the underlying data transport, the storage require to ingest and store messages is
just that of Kafka. See [Kafka performance](http://kafka.apache.org/performance.html) figures for information.
In general, Kafka is not nearly a bottleneck and can be sized like any other usage of Kafka.

# Batch Layer

The Batch Layer's particular role is model building, and the element that is of most interest to benchmark
are likely the model building processes implemented in the app tier, on top of MLlib. Here again, the 
resources required to build a model over a certain amount of data are just that of the underlying MLlib
implementations of ALS, k-means and decision forests.

Any performance tuning or benchmarks for MLlib will be valid for the Batch Layer's pre-made implementations
on MLlib, and there is nothing different to know for Oryx.

## JVM Tuning

Choosing the number of Spark executors, cores and memory is a topic in its own right.

More executors means, naturally, more cores and memory. The number should not exceed the number of machines 
in the cluster; it can be less. See `oryx.batch.streaming.num-executors`.

More cores means potentially more parallel processing. It can usefully be up to 1/3 or 1/2of the total number of 
tasks in a typical model building process. You can observe the number of tasks and thus inherent parallelism
in the Spark UI of a Batch layer run. Beyond this count, more cores doesn't add much parallelism. Fewer is OK 
and simply increases the run time. Of course, enough cores should be available to get the batch process completed 
comfortably within the batch interval. The number of cores is configured by 
`oryx.batch.streaming.executor-cores`.

If your jobs are running out of memory, the driver or executors may need more memory. More memory may be
helpful if you notice in the "Storage" tab of your batch layer that some cached RDDs show as less than 100% 
cached. See `oryx.batch.streaming.executor-memory`.

The `--jvm-args` flag to `oryx-run.sh` can be used to set JVM memory parameters for all JVM processes. 
For example, `-XX:+UseG1GC` is a good default garbage collection setting.

# Serving Layer

The REST API is powered by Tomcat. Its configuration is not exposed to the user, but is already reasonably
tuned internally for its workload. The Tomcat container itself introduces little overhead and is not 
a concern for performance. 

What is likely of interest is performance of CPU-intensive app tier implementations provided in the project,
rather than the framework itself.

## Benchmark: Alternating Least Squares Recommendation

Since most operations in the ALS app Serving Layer are performed on a huge matrix in memory in real-time,
this app is the most challenging to scale. General rules of thumb follow below.

To run similar benchmarks, use `LoadBenchmark`, which has some configuration parameters:

```
mvn -DskipTests clean install
cd app/oryx-app-serving
...
mvn -Pbenchmark \
 -Doryx.test.als.benchmark.users=1000000 \
 -Doryx.test.als.benchmark.items=5000000 \
 -Doryx.test.als.benchmark.features=250 \
 -Doryx.test.als.benchmark.lshSampleRate=0.3 \
 -Doryx.test.als.benchmark.workers=2 \
 integration-test
```

### Memory

- Memory requirements scale linearly with (users + items) x features
- `-XX:+UseStringDeduplication` helps a lot (reflected below)
- At scale, 1M users or items ~= 500-1000M of heap required, depending on features

Example steady-state heap usage:

| Features | Users+Items (M) | Heap (MB) |
| --------:| ---------------:| ---------:|
|  50      |  2              |  1400     |
|  50      |  6              |  2600     |
|  50      | 21              |  7500     |
| 250      |  2              |  3000     |
| 250      |  6              |  7500     |
| 250      | 21              | 25800     |


### Request Latency, Throughput

- Recommend and similarity computation time scales linearly with items x features
- A single request is parallelized across CPUs; max throughput and minimum latency is already achieved at about 1-2 concurrent requests
- Locality sensitive hashing decreases processing time roughly linearly; 0.33 ~= 1/0.33 ~= 3x faster (setting too low adversely affects result quality)

Below are representative throughput / latency measurements for the `/recommend` endpoint using  
a 32-core Intel Xeon 2.3GHz (Haswell), OpenJDK 8 and flags `-XX:+UseG1GC -XX:+UseStringDeduplication`. 
Heap size was comfortably large enough for the data set in each case. 
The tests were run with 1-3 concurrent request at a time, as necessary to achieve near-full CPU utilization.

*With LSH (sample rate = 0.3)*

| Features | Items (M) | Throughput (qps) | Latency (ms) |
| --------:| ---------:| ----------------:| ------------:|
|  50      |  1        | 437              |    7         |
| 250      |  1        | 151              |   13         |
|  50      |  5        |  84              |   24         |
| 250      |  5        |  36              |   56         |
|  50      | 20        |  14              |   69         |
| 250      | 20        |   6              |  162         |

*Without LSH (sample rate = 1.0)*

| Features | Items (M) | Throughput (qps) | Latency (ms) |
| --------:| ---------:| ----------------:| ------------:|
|  50      |  1        |  74              |   27         |
| 250      |  1        |  23              |   44         |
|  50      |  5        |  13              |   80         |
| 250      |  5        |   5              |  191         |
|  50      | 20        |   4              |  282         |
| 250      | 20        |   1              |  708         |

## JVM Tuning

Running the Serving layer(s) on machines with more available cores generally means more requests can
be served in parallel. In the case of ALS, some requests like `/recommend` can use multiple cores in one
request.

Memory requirements are dominated by the need to load a model in memory. For large models like ALS this
may mean ensuring that the Serving layer memory setting is comfortably high enough to hold the model without
GC thrashing. See `oryx.serving.memory`.

`-XX:+UseG1GC` remains a good garbage collection setting to supply with `--jvm-args`.  
`-XX:+UseStringDeduplication` can reduce memory requirements by about 20%.

# Speed Layer

The Speed layer _driver_ process is as memory-hungry as the Serving layer since it also loads a model
into memory. The memory of the driver process, controlled by `oryx.speed.streaming.driver-memory`, may need
to be set like the Serving layer memory, and may benefit from the same JVM flags.

It is also a Spark Streaming job and so needs executors configures like the Batch layer. However, there
is generally much less processing done by the Speed layer's executors, but at a much lower latency requirement.

Executors will have to be sized to consume input Kafka partitions fully in parallel; the number of cores
times number of executors should be at least the number of Kafka partitions.
