import sbt._
import sbt.Keys._

object SparkGlove extends Build {

  val sparkVersion = "1.1.0"

  lazy val root = Project(id = "spark-glove", base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "spark-glove",
      version := "0.1-SNAPSHOT",
      scalaVersion := "2.10.4",
      scalacOptions := Seq("-deprecation", "-unchecked", "-encoding", "utf8", "-Xlint"),
      resolvers ++= Seq("snapshots", "releases").map(Resolver.sonatypeRepo),
      libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core" % sparkVersion,
        "org.apache.spark" %% "spark-mllib" % sparkVersion,
        "org.apache.commons" % "commons-math3" % "3.3",
        "org.scalanlp" %% "breeze" % "0.10",
        // native libraries are not included by default. add this if you want them (as of 0.7)
        // native libraries greatly improve performance, but increase jar sizes.
        "org.scalanlp" %% "breeze-natives" % "0.10")))
}
