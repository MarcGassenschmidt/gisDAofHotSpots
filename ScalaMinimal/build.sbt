name := "ScalaMinimal"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "net.postgis" % "postgis-jdbc" % "2.1.7.2"
libraryDependencies += "com.typesafe.slick" % "slick_2.10.1" % "2.0.0-M1"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.1" % "provided"
libraryDependencies += "com.azavea.geotrellis" % "geotrellis-tasks_2.10" % "0.8.0"