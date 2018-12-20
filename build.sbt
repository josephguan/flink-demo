import sbt.Keys._

// flink examples
lazy val examples = project.in(file("examples")).settings(name := "examples").
  settings(Common.settings: _*).
  settings(libraryDependencies ++= Dependencies.test).
  settings(libraryDependencies ++= Dependencies.flink)

// kafka producers for testing
lazy val kafka = project.in(file("kafka")).settings(name := "kafka").
  settings(Common.settingsIncludeScala: _*).
  settings(libraryDependencies ++= Dependencies.test).
  settings(libraryDependencies ++= Dependencies.kafka)

// spark streaming examples
lazy val spark = project.in(file("spark")).settings(name := "spark").
  settings(Common.settings: _*).
  settings(libraryDependencies ++= Dependencies.test).
  settings(libraryDependencies ++= Dependencies.spark)

// aggregate all sub projects
lazy val all = (project in file(".")).settings(name := "all").
  settings(Common.settings: _*).
  aggregate(examples, kafka, spark)


