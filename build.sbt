val catsv = "0.4.1"
val sparkCats = "1.6.1"
val sparkDataset = "1.6.1"
val sparkDataFrame = "1.5.2"
val sparkTesting = "0.3.1"
val scalatest = "2.2.5"
val shapeless = "2.3.0"
val scalacheck = "1.12.5"

lazy val root = Project("frameless", file("." + "frameless")).in(file("."))
  .aggregate(common, cats, dataset, dataframe)
  .settings(framelessSettings: _*)
  .settings(noPublishSettings: _*)

lazy val common = project
  .settings(framelessSettings: _*)
  .settings(libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql"          % sparkDataFrame,
    "com.holdenkarau"  %% "spark-testing-base" % (sparkDataFrame + "_" + sparkTesting) % "test"))

lazy val cats = project
  .settings(name := "frameless-cats")
  .settings(framelessSettings: _*)
  .settings(publishSettings: _*)
  .settings(libraryDependencies ++= Seq(
    "org.typelevel"    %% "cats"       % catsv,
    "org.apache.spark" %% "spark-core" % sparkCats))

lazy val dataset = project
  .settings(name := "frameless-dataset")
  .settings(framelessSettings: _*)
  .settings(publishSettings: _*)
  .settings(libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkDataset,
    "org.apache.spark" %% "spark-sql"  % sparkDataset))
  .dependsOn(common % "test->test;compile->compile")

lazy val dataframe = project
  .settings(name := "frameless-dataframe")
  .settings(framelessSettings: _*)
  .settings(publishSettings: _*)
  .settings(libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkDataFrame,
    "org.apache.spark" %% "spark-sql"  % sparkDataFrame))
  .settings(sourceGenerators in Compile <+= (sourceManaged in Compile).map(Boilerplate.gen))
  .dependsOn(common % "test->test;compile->compile")

lazy val framelessSettings = Seq(
  organization := "io.github.adelbertc",
  scalaVersion := "2.11.8",
  version := "0.1.0-SNAPSHOT",
  scalacOptions ++= commonScalacOptions,
  licenses += ("Apache-2.0", url("http://opensource.org/licenses/Apache-2.0")),
  testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oDF"),
  libraryDependencies ++= Seq(
    "com.chuusai" %% "shapeless" % shapeless,
    "org.scalatest" %% "scalatest" % scalatest % "test",
    "org.scalacheck" %% "scalacheck" % scalacheck % "test"
  ),
  fork in Test := false,
  parallelExecution in Test := false
) ++ warnUnusedImport

lazy val commonScalacOptions = Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xlint:-missing-interpolator,_",
  "-Yinline-warnings",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-language:existentials",
  "-language:experimental.macros",
  "-language:implicitConversions",
  "-language:higherKinds",
  "-Xfuture")

lazy val warnUnusedImport = Seq(
  scalacOptions ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 10)) =>
        Seq()
      case Some((2, n)) if n >= 11 =>
        Seq("-Ywarn-unused-import")
    }
  },
  scalacOptions in (Compile, console) ~= {_.filterNot("-Ywarn-unused-import" == _)},
  scalacOptions in (Test, console) <<= (scalacOptions in (Compile, console))
)

lazy val publishSettings = Seq(
  publishMavenStyle := true,
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
  },
  publishArtifact in Test := false,
  pomIncludeRepository := Function.const(false),
  pomExtra in Global := {
    <url>https://github.com/adelbertc/frameless</url>
    <scm>
      <url>git@github.com:adelbertc/frameless.git</url>
      <connection>scm:git:git@github.com:adelbertc/frameless.git</connection>
    </scm>
    <developers>
      <developer>
        <id>adelbertc</id>
        <name>Adelbert Chang</name>
        <url>https://github.com/adelbertc/</url>
      </developer>
    </developers>
  }
)

lazy val noPublishSettings = Seq(
  publish := (),
  publishLocal := (),
  publishArtifact := false
)
