name := "frameless"

version := "0.0.1"

licenses := Seq(("Apache-2.0", url("http://opensource.org/licenses/Apache-2.0")))

val sparkVersion = "1.6.0"

scalaVersion := "2.11.7"

scalacOptions := Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xlint",
  "-Yinline-warnings",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-unused-import",
  "-Ywarn-value-discard",
  "-Xfatal-warnings",
  "-Xfuture")

libraryDependencies := Seq(
  "org.scalatest" % "scalatest_2.11" % "2.2.5" % "test",
  "com.chuusai" %% "shapeless" % "2.2.5",
  "eu.timepit" %% "refined" % "0.3.2",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scala-lang" % "scala-reflect" % scalaVersion.value % "provided")

fork in Test := true

sourceGenerators in Compile <+= (sourceManaged in Compile).map(Boilerplate.gen)
