title: Performance

_Work in Progress_

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

# Serving Layer

The REST API is powered by Tomcat. Its configuration is not exposed to the user, but is already reasonably
tuned internally for its workload. The Tomcat container itself introduces little overhead and is not 
a concern for performance. 

What is likely of interest is performance of CPU-intensive app tier implementations provided in the project,
rather than the framework itself.

## Alternating Least Squares Recommendation

Since most operations in the ALS app Serving Layer are performed on a huge matrix in memory in real-time,
this app is the most challenging to scale. General rules of thumb:


### Memory

- Memory requirements scale linearly with (users + items) x features
- At scale, with ~200 features, 1M users or items ~= 1GB of heap needed

Example steady-state heap usage (Java 8):

| Features | Users+Items (M) | Heap (MB) |
| --------:| ---------------:| ---------:|
|  50      |  2              |  2064     |
|  50      |  6              |  3399     |
|  50      | 21              | 10605     |
| 250      |  2              |  3666     |
| 250      |  6              |  8200     |
| 250      | 21              | 27360     |


### Request Latency, Throughput

- Recommend and similarity computation time scales linearly with items x features
- A single request is parallelized across CPUs; max throughput and minimum latency is already achieved at about 1-2 concurrent requests
- Locality sensitive hashing decreases processing time roughly linearly; 0.33 ~= 1/0.33 ~= 3x faster (setting too low adversely affects result quality)

Example throughput / latency for the `/recommend` endpoint (Java 8, 16-CPU Intel Xeon 2.5GHz):

*With LSH (sample rate = 0.3)*

| Features | Items (M) | Throughput (qps) | Latency (ms) |
| --------:| ---------:| ----------------:| ------------:|
|  50      |  1        | 182              |   22         |
| 250      |  1        |  64              |   31         |
|  50      |  5        |  41              |   49         |
| 250      |  5        |  15              |  131         |
|  50      | 20        |  11              |  175         |
| 250      | 20        |   4              |  534         |

*Without LSH (sample rate = 1.0)*

| Features | Items (M) | Throughput (qps) | Latency (ms) |
| --------:| ---------:| ----------------:| ------------:|
|  50      |  1        |  44              |   45         |
| 250      |  1        |  18              |  109         |
|  50      |  5        |  11              |  186         |
| 250      |  5        |   4              |  523         |
|  50      | 20        |   3              |  763         |
| 250      | 20        |   1              | 2167         |
