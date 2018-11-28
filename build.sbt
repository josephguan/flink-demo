import sbt.Keys._


lazy val examples = project.in(file("examples")).settings(name := "examples").
  settings(Common.settings: _*).
  settings(libraryDependencies ++= Dependencies.test).
  settings(libraryDependencies ++= Dependencies.flink)


lazy val all = (project in file(".")).settings(name := "all").
  settings(Common.settings: _*).
  aggregate(examples)


