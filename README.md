# Streaming Lab

This is my experiment lab for studying streaming frameworks, such as flink, spark-streaming, etc.
It includes some examples currently. In the near future, I will introduce more experiments...


## How To Compile

Run ```sbt assembly``` to compile, run tests and package all sub-projects.

If you want to operate specific sub-project, use ```project``` command in sbt shell to switch sub-project:
```shell
> projects          // list all sub-projects
> project examples  // switch to 'examples' sub-project
> project all       // switch to 'all' project which including all sub-projects
```


## Subprojects Overview

| subproject                          | description                                                 |
|-------------------------------------|-------------------------------------------------------------|
| [flink](./flink/README.md)          | Some examples for using flink in different ways.            |
| [kafka](./kafka/README.md)          | Some kafka producers used in flink and spark examples.      |
| [spark](./spark/README.md)          | Some examples for spark streaming and structured streaming. |


## License
This project is licensed under the terms of the Apache 2.0 license.

