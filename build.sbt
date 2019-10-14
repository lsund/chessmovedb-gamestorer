scalaVersion := "2.12.10"
name := "chessmovedb-gamestorer"
organization := "com.github.lsund"
version := "1.0.0"

scalacOptions += "-Ypartial-unification"

// Dependencies
libraryDependencies += "org.typelevel" %% "cats-core" % "2.0.0"
libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.4.2"
libraryDependencies += "org.apache.kafka" %% "kafka" % "2.3.0"

libraryDependencies += "org.typelevel" %% "cats-core" % "2.0.0"

scalacOptions += "-Ypartial-unification" // 2.11.9+

libraryDependencies ++= Seq(
  "org.tpolecat" %% "doobie-core"      % "0.8.4",
  "org.tpolecat" %% "doobie-postgres"  % "0.8.4"
)

val circeVersion = "0.12.2"
libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)
