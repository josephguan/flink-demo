# SubProject: examples

## How to use
for instance:
```shell
> ./flink run --class gx.flink.examples.table.Kafka09CsvTableExample flink-snippet-examples_0.0.1.jar --bootstrap.servers zdh16en:9092 --input-topic gxtest --output-topic gxtestout --zookeeper.connect zdh16en:2181/kafka --group.id gxhello --interval 5
```

monitor print output:
```shell
> tail -f log/flink-*-taskexecutor-*.out
```

monitor kafka output(topic):
```shell
> kafka-console-consumer --bootstrap-server zdh16en:9092 --topic gxtestout  --zookeeper zdh16en:2181/kafka
```


## Examples

#### [Kafka09StreamWordCountExample](./src/main/scala/gx/flink/examples/streaming/Kafka09StreamWordCountExample.scala)
A word count example for kafka(version 0.9) source.
Use SimpleWordProducer in [kafka](../kafka/README.md) subproject to generate test data.


#### [SocketStreamWordCountExample](./src/main/scala/gx/flink/examples/streaming/SocketStreamWordCountExample.scala)
A word count example for socket source.


#### [Kafka09JsonTableExample](./src/main/scala/gx/flink/examples/sql/Kafka09JsonTableExample.scala)
Process kafka source in json format using sql.
Use SimpleJsonProducer in [kafka](../kafka/README.md) subproject to generate test data.


#### [Kafka09CsvTableExample](./src/main/scala/gx/flink/examples/sql/Kafka09CsvTableExample.scala)
Process kafka source in csv format using sql.
Use SimpleCsvProducer in [kafka](../kafka/README.md) subproject to generate test data.
In this case, we used self-defined TableSource, Kafka09CsvTableSource, since flink do not have out-of-box supporting for csv format from kafka source.


#### [TwoStreamsWindowJoinExample](./src/main/scala/gx/flink/examples/sql/TwoStreamsWindowJoinExample.scala)
Demonstrates joining two streams using sql.


#### Kafka09JoinDimensionTableExample
**TBD** demonstrates stream table join with dimension table using table api.



