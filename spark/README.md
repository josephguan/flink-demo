# SubProject: spark

This is a collection of example codes for spark streaming and structured streaming.

## How to use
executing the jar:
```shell
> ./spark-submit --master local[1] --class gx.spark.examples.structured.StructuredNetworkWordCount streaming-spark_0.0.1.jar localhost 9999
```


## Structured Streaming Examples

#### [StructuredNetworkWordCount](./src/main/scala/gx/spark/examples/structured/StructuredNetworkWordCount.scala)
A word count example for socket source.


## SQL Examples

#### [SqlNetworkWordCount](./src/main/scala/gx/spark/examples/sql/SqlNetworkWordCount.scala)
A word count example for socket source, using sql with group by window statement.

#### [SqlJoinWithStatic](./src/main/scala/gx/spark/examples/sql/SqlJoinWithStatic.scala)
A example for joining stream with static(dimension) table. 

#### [SqlWatermark](./src/main/scala/gx/spark/examples/sql/SqlWatermark.scala)
Figure out how watermark works. 
Launch the program and set the watermark to 10 seconds and group by window to 10 seconds.
```shell
// Usage: SqlWatermark <hostname1> <port1> <watermark> <window>
> ./spark-submit --master local[1] --class gx.spark.examples.sql.SqlWatermark /root/joe/streaming-spark_0.0.1.jar localhost 9991 10 10
```
*note: input format is "word,minute,second"*

input                        |output             |explain
-----------------------------|-------------------|--------------------------
a,10,1<br>a,10,9<br>a,10,10  |nothing            |watermark<10:00,window start at 10:00
a,10,11<br>a,10,19<br>a,10,20|nothing            |watermark<10:10,window=[10:00,10:10) is not ready
a,10,21                      |a,10:00-10:10,2    |watermark<10:11,window=[10:00,10:10) tumbled
a,10,31                      |a,10:10-10:20,3    |watermark<10:21,window=[10:10,10:20) tumbled
a,10,2                       |nothing            |watermark<10:21,this record is out of date
a,10,22<br>a,10,23<br>a,10,24|nothing            |watermark<10:21,window=[10:20,10:30) is not ready
a,10,41                      |a,10:20-10:30,5    |watermark<10:31,window=[10:20,10:30) tumbled

#### [SqlJoinWithStream](./src/main/scala/gx/spark/examples/sql/SqlJoinWithStream.scala)
Figure out how steam-stream join works.
```shell
// Usage: SqlJoinWithStream <hostname1> <port1> <watermark1> <hostname2> <port2> <watermark2> <delay>
> ./spark-submit --master local[1] --class gx.spark.examples.sql.SqlJoinWithStream /root/joe/streaming-spark_0.0.1.jar localhost 9991 10 localhost 9992 10 30
```

stream1           |stream2                        |output                                |explain
------------------|-------------------------------|--------------------------------------|--------------------------
a,10,0            |a,10,29<br>a,10,30<br>a,10,31  |a,10:00,a,10:29<br>a,10:00,a,10:30    |stream1 can tolerate 30 seconds delay of stream2
b,10,0<br>d,10,41 |d,10,41                        |d,10:41,d,10:41<br>b,10:00,null,null  |stream2 watermark<10:31, so only data later than 10:01 is saved. b(10:00) in stream1 can not join with anything.


***note1:*** 

input format is "word,minute,second", stream1 LEFT JOIN stream2*

***note2:*** 

In the current implementation in the micro-batch engine, watermarks are advanced at the end of a micro-batch, 
and the next micro-batch uses the updated watermark to clean up state and output outer results. 
Since we trigger a micro-batch only when there is new data to be processed, the generation of the outer result may 
get delayed if there no new data being received in the stream. In short, if any of the two input streams being joined 
does not receive data for a while, the outer (both cases, left or right) output may get delayed.