import sbt.Keys._
import sbtassembly.AssemblyKeys.{assemblyJarName, assemblyOption}


organization := "io.gx"
name := "flink-demo"
version := "0.1"

scalaVersion := "2.11.8"
javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-encoding", "UTF-8")
scalacOptions ++= Seq("-deprecation", "-unchecked")

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := s"${name.value}-${version.value}.jar"

val flinkVersion = "1.8.0"

libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-table-planner" % flinkVersion,
  "org.apache.flink" %% "flink-table-api-scala-bridge" % flinkVersion,
  "org.scalatest" %% "scalatest" % "3.0.1" % "test")



