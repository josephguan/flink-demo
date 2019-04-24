# Flink Demo

Examples for Flink v1.8.0

## Get Started

1. Run ```sbt assembly``` to build the jar.
2. Run ```flink run --class xxx flink-demo-0.1.jar``` to submit the jar to flink.
3. Run ```tail -f log/flink-*-taskexecutor-*.out``` to check the output.


## Demo List

```shell
├── api
│   └── table
│       ├── TableAggregationOverWindow.scala
│       ├── TableGroupBy.scala
│       ├── TableGroupByTimeWindow.scala
│       ├── TableJoin.scala
│       ├── TableJoinAndGroupByTimeWindow.scala
│       ├── TableJoinDimension.scala
│       ├── TableJoinDimensionOld.scala
│       ├── TableJoinTimeWindow.scala
│       └── TableUnion.scala
└── connector

```

## License
This project is licensed under the terms of the Apache 2.0 license.
