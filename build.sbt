name := "frameless"

version := "0.0.1"

scalaVersion := "2.10.5"

licenses += ("Apache-2.0", url("http://opensource.org/licenses/Apache-2.0"))

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases")
)

val sparkVersion = "1.3.0"

libraryDependencies ++= Seq(
  compilerPlugin("org.scalamacros"  % "paradise_2.10.5" % "2.0.1"),

  "com.chuusai"       %% "shapeless"          % "2.2.0-RC4",
  "org.apache.spark"  %% "spark-core"         % sparkVersion    % "provided",
  "org.apache.spark"  %% "spark-sql"          % sparkVersion    % "provided"
)

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard"
)

