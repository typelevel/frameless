val home = "https://github.com/adelbertc/frameless"
val repo = "git@github.com:adelbertc/frameless.git"
val org = "github.com/adelbertc/frameless"
val license = ("Apache-2.0", url("http://opensource.org/licenses/Apache-2.0"))

val catsv = "0.4.1"
val sparkCats = "1.6.0"
val sparkDataset = "1.6.0"
val sparkDataFrame = "1.5.2"
val sparkTesting = "0.3.1"
val scalatest = "2.2.5"
val shapeless = "2.2.5"
val scalacheck = "1.12.5"
val scalaVersions = Seq("2.10.6", "2.11.7")

lazy val root = Project("frameless", file("." + "frameless")).in(file("."))
  .aggregate(common, cats, dataset, dataframe)
  .settings(framelessSettings: _*)

lazy val common = project
  .settings(framelessSettings: _*)
  .settings(libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql"          % sparkDataFrame,
    "com.holdenkarau"  %% "spark-testing-base" % (sparkDataFrame + "_" + sparkTesting) % "test"))

lazy val cats = project
  .settings(framelessSettings: _*)
  .settings(libraryDependencies ++= Seq(
    "org.typelevel"    %% "cats"       % catsv,
    "org.apache.spark" %% "spark-core" % sparkCats))

lazy val dataset = project
  .settings(framelessSettings: _*)
  .settings(libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkDataset,
    "org.apache.spark" %% "spark-sql"  % sparkDataset))
  .dependsOn(common % "test->test;compile->compile")

lazy val dataframe = project
  .settings(framelessSettings: _*)
  .settings(libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkDataFrame,
    "org.apache.spark" %% "spark-sql"  % sparkDataFrame))
  .settings(sourceGenerators in Compile <+= (sourceManaged in Compile).map(Boilerplate.gen))
  .dependsOn(common % "test->test;compile->compile")

lazy val framelessSettings = Seq(
  scalaVersion := scalaVersions.last,
  organization := org,
  crossScalaVersions := scalaVersions,
  scalacOptions ++= commonScalacOptions,
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
  "-Xlint",
  "-Yinline-warnings",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-language:existentials",
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
