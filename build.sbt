import Keys._
import sbtassembly.Plugin.AssemblyKeys._

name := "flink-snippet"

version := "1.0.0"

scalaVersion in ThisBuild := "2.11.8"

organization in ThisBuild := "com.zte.bigdata"

val flinkVersion = "1.6.1"

//libraryDependencies += "org.apache.flink" %% "flink-core" % flinkVersion % "provided"

libraryDependencies += "org.apache.flink" %% "flink-scala" % flinkVersion % "provided"

//libraryDependencies += "org.apache.flink" %% "flink-streaming-core" % flinkVersion % "provided"

libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided"

libraryDependencies += "org.apache.flink" %% "flink-table" % flinkVersion

libraryDependencies += "org.apache.flink" %% "flink-connector-kafka-0.10" % flinkVersion

libraryDependencies += "org.apache.flink" % "flink-json" % flinkVersion

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

assemblySettings

jarName in assembly := s"${name.value}.jar"

test in assembly := {}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
