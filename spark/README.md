# SubProject: spark

This is a collection of example codes for spark streaming and structured streaming.

## How to use
executing the jar:
```shell
> ./spark-submit --class gx.flink.examples.table.Kafka09CsvTableExample flink-snippet-examples_0.0.1.jar --bootstrap.servers zdh16en:9092 --input-topic gxtest --output-topic gxtestout --zookeeper.connect zdh16en:2181/kafka --group.id gxhello --interval 5
```

monitoring print output:
```shell
> tail -f log/flink-*-taskexecutor-*.out
```

monitoring kafka output(topic):
```shell
> kafka-console-consumer --bootstrap-server zdh16en:9092 --topic gxtestout  --zookeeper zdh16en:2181/kafka
```


## Structured Streaming Examples

#### [StructuredNetworkWordCount](./src/main/scala/gx/spark/examples/structured/StructuredNetworkWordCount.scala)
A word count example for socket source.


## SQL Examples

