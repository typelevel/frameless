val sparkVersion = "2.0.2"
val catsv = "0.9.0"
val scalatest = "3.0.1"
val shapeless = "2.3.2"
val scalacheck = "1.13.4"

lazy val root = Project("frameless", file("." + "frameless")).in(file("."))
  .aggregate(core, cats, dataset, docs)
  .settings(framelessSettings: _*)
  .settings(noPublishSettings: _*)

lazy val core = project
  .settings(name := "frameless-core")
  .settings(framelessSettings: _*)
  .settings(warnUnusedImport: _*)
  .settings(publishSettings: _*)

lazy val cats = project
  .settings(name := "frameless-cats")
  .settings(framelessSettings: _*)
  .settings(warnUnusedImport: _*)
  .settings(publishSettings: _*)
  .settings(libraryDependencies ++= Seq(
    "org.typelevel"    %% "cats"       % catsv,
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided"))

lazy val dataset = project
  .settings(name := "frameless-dataset")
  .settings(framelessSettings: _*)
  .settings(warnUnusedImport: _*)
  .settings(framelessTypedDatasetREPL: _*)
  .settings(publishSettings: _*)
  .settings(libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided"
  ))
  .dependsOn(core % "test->test;compile->compile")

lazy val docs = project
  .settings(framelessSettings: _*)
  .settings(noPublishSettings: _*)
  .settings(tutSettings: _*)
  .settings(crossTarget := file(".") / "docs" / "target")
  .settings(libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql"  % sparkVersion
  ))
  .settings(
    sourceDirectory in GitBook := sourceDirectory.value / ".." / "target" / "tut",
    siteSubdirName  in GitBook := "../../")
  .enablePlugins(GitBookPlugin)
  .dependsOn(dataset, cats)

lazy val framelessSettings = Seq(
  organization := "io.github.adelbertc",
  scalaVersion := "2.11.8",
  scalacOptions ++= commonScalacOptions,
  licenses += ("Apache-2.0", url("http://opensource.org/licenses/Apache-2.0")),
  testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oDF"),
  libraryDependencies ++= Seq(
    "com.chuusai" %% "shapeless" % shapeless,
    "org.scalatest" %% "scalatest" % scalatest % "test",
    "org.scalacheck" %% "scalacheck" % scalacheck % "test"),
  fork in Test := false,
  parallelExecution in Test := false
)

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
  scalacOptions in (Test, console) := (scalacOptions in (Compile, console)).value
)

lazy val framelessTypedDatasetREPL = Seq(
  initialize ~= { _ => // Color REPL
    val ansi = System.getProperty("sbt.log.noformat", "false") != "true"
    if (ansi) System.setProperty("scala.color", "true")
  },
  initialCommands in console :=
    """
      |import org.apache.spark.{SparkConf, SparkContext}
      |import org.apache.spark.sql.SparkSession
      |import frameless.functions.aggregate._
      |
      |val conf = new SparkConf().setMaster("local[*]").setAppName("frameless repl").set("spark.ui.enabled", "false")
      |val spark = SparkSession.builder().config(conf).appName("REPL").getOrCreate()
      |
      |import spark.implicits._
      |
      |spark.sparkContext.setLogLevel("WARN")
      |
      |import frameless.TypedDataset
      |implicit val sqlContenxt = spark.sqlContext
    """.stripMargin,
  cleanupCommands in console :=
    """
      |spark.stop()
    """.stripMargin
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

lazy val credentialSettings = Seq(
  // For Travis CI - see http://www.cakesolutions.net/teamblogs/publishing-artefacts-to-oss-sonatype-nexus-using-sbt-and-travis-ci
  credentials ++= (for {
    username <- Option(System.getenv().get("SONATYPE_USERNAME"))
    password <- Option(System.getenv().get("SONATYPE_PASSWORD"))
  } yield Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", username, password)).toSeq
)

syslinkReadme := syslinkReadmeImpl.value
lazy val syslinkReadme = taskKey[Unit]("syslink for website generation")
lazy val syslinkReadmeImpl = Def.task {
  val from = baseDirectory.value / "README.md"
  val to   = baseDirectory.value / "docs" / "src" / "main" / "tut" / "README.md"
  try {
    if (!to.exists)
      java.nio.file.Files.createSymbolicLink(to.toPath, from.toPath)
  } catch {
    case e: UnsupportedOperationException =>
      // If the OS doesn't support symbolic links, copy instead...
      sbt.IO.copy(List((from, to)), overwrite = true, preserveLastModified = true)
  }
}
