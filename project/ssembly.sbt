/* @see https://github.com/sbt/sbt-assembly */
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")

/* @see https://github.com/jrudolph/sbt-dependency-graph */
//addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.0")

//import sbtassembly.AssemblyKeys._

//assemblySettings
//
//assembleArtifact in assemblyPackageScala := false
//
//run in Compile <= Defaults.runTask(fullClasspath in Compile, mainClass in ())
//runMain in Compile <= Defaults.runMainTask(fullClasspath in Compile, runner)