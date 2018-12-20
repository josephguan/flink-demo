# SubProject: spark

This is a collection of example codes for spark streaming and structured streaming.

## How to use
executing the jar:
```shell
> ./ spark-submit --master local[1] --class gx.spark.examples.structured.StructuredNetworkWordCount streaming-spark_0.0.1.jar localhost 9999
```


## Structured Streaming Examples

#### [StructuredNetworkWordCount](./src/main/scala/gx/spark/examples/structured/StructuredNetworkWordCount.scala)
A word count example for socket source.


## SQL Examples

