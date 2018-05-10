//name := "scala-spark-stream"
//
//version := "0.1"
//
//scalaVersion := "2.11.12"
//
//scalaSource in Compile := baseDirectory.value / "src"

//net.virtualvoid.sbt.graph.Plugin.greapSettings

lazy val root = (project in file("."))
  .settings( name := "scala-spark-stream",
    organization := "com.yunjae",
    scalaVersion := "2.11.12",
    version := "0.1.0-SNAPSHOT",
    mainClass in assembly := Some("com.yunjae.spark.stream.strandalone.MainApp") )


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "org.apache.spark" %% "spark-sql" % "2.3.0",
  "org.apache.spark" %% "spark-streaming" % "2.3.0",
  // https://mvnrepository.com/artifact/net.liftweb/lift-json
  "net.liftweb" %% "lift-json" % "3.3.0-M2",
  "log4j" % "log4j" % "1.2.17",
  "org.scalatest" %% "scalatest" % "3.0.5" % Test
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}


/*
resolvers in Global ++= Seq( "Sbt plugins" at "https://dl.bintray.com/sbt/sbt-plugin-releases",
  "Maven Central Server" at "http://repo1.maven.org/maven2",
  "JBoss" at "https://repository.jboss.org/" )*/
