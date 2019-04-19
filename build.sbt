import sbt.Keys._

// flink examples
lazy val flink = project.in(file("flink")).settings(name := "flink").
  settings(Common.settings: _*).
  settings(libraryDependencies ++= Dependencies.test).
  settings(libraryDependencies ++= Dependencies.flink)

// kafka producers for testing
lazy val kafka = project.in(file("kafka")).settings(name := "kafka").
  settings(Common.settingsIncludeScala: _*).
  settings(libraryDependencies ++= Dependencies.test).
  settings(libraryDependencies ++= Dependencies.kafka)

// spark structured streaming examples
lazy val spark = project.in(file("spark")).settings(name := "spark").
  settings(Common.settings: _*).
  settings(libraryDependencies ++= Dependencies.test).
  settings(libraryDependencies ++= Dependencies.spark)

// blink examples
lazy val blink = project.in(file("blink")).settings(name := "blink").
  settings(Common.settings: _*).
  settings(libraryDependencies ++= Dependencies.test).
  settings(libraryDependencies ++= Dependencies.blink)

// aggregate all sub projects
lazy val all = (project in file(".")).settings(name := "all").
  settings(Common.settings: _*).
  aggregate(flink, kafka, spark)


