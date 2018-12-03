# SubProject: examples

## How to use
for instance:
```shell
> ./flink run --class gx.flink.examples.table.Kafka09CsvTableExample flink-snippet-examples_0.0.1.jar --bootstrap.servers zdh16en:9092 --input-topic gxtest --output-topic gxtestout --zookeeper.connect zdh16en:2181/kafka --group.id gxhello --interval 5
```


## Examples

#### [Kafka09StreamWordCountExample](./src/main/scala/gx/flink/examples/streaming/Kafka09StreamWordCountExample.scala)
A word count example for kafka(version 0.9) source.
Use SimpleWordProducer in [kafka](../kafka/README.md) subproject to generate test data.


#### [SocketStreamWordCountExample](./src/main/scala/gx/flink/examples/streaming/SocketStreamWordCountExample.scala)
A word count example for socket source.


#### [Kafka09JsonTableExample](./src/main/scala/gx/flink/examples/streaming/Kafka09JsonTableExample.scala)
Process kafka source in json format using table api.
Use SimpleJsonProducer in [kafka](../kafka/README.md) subproject to generate test data.


#### [Kafka09CsvTableExample](./src/main/scala/gx/flink/examples/streaming/Kafka09CsvTableExample.scala)
Process kafka source in csv format using table api.
Use SimpleCsvProducer in [kafka](../kafka/README.md) subproject to generate test data.
In this case, we used self-defined TableSource, Kafka09CsvTableSource, since flink do not have out-of-box supporting for csv format from kafka source.


#### Kafka09TowStreamJoinExample
**TBD** demonstrates two streams join using table api.


#### Kafka09JoinDimensionTableExample
**TBD** demonstrates stream table join with dimension table using table api.



