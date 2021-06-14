val sparkVersion = "3.1.2"
val catsCoreVersion = "2.6.1"
val catsEffectVersion = "2.4.0"
val catsMtlVersion = "0.7.1"
val scalatest = "3.2.9"
val scalatestplus = "3.1.0.0-RC2"
val shapeless = "2.3.7"
val scalacheck = "1.15.4"
val irrecVersion = "0.4.0"

val Scala212 = "2.12.14"

ThisBuild / crossScalaVersions := Seq(Scala212)
ThisBuild / scalaVersion := (ThisBuild / crossScalaVersions).value.last

ThisBuild / githubWorkflowPublishTargetBranches := Seq()

ThisBuild / githubWorkflowArtifactUpload := false

ThisBuild / githubWorkflowBuild := Seq(
  WorkflowStep.Use(UseRef.Public("actions", "setup-python", "v2"),
                   name = Some("Setup Python"),
                   params = Map("python-version" -> "3.x")
  ),
  WorkflowStep.Run(List("pip install codecov"),
                   name = Some("Setup codecov")
  ),
  WorkflowStep.Sbt(List("coverage", "test", "coverageReport"),
                   name = Some("Test & Compute Coverage")
  ),
  WorkflowStep.Run(List("codecov -F ${{ matrix.scala }}"),
                   name = Some("Upload Codecov Results")
  )
)

ThisBuild / githubWorkflowAddedJobs ++= Seq(
  WorkflowJob(
    "docs",
    "Documentation",
    githubWorkflowJobSetup.value.toList ::: List(
      WorkflowStep.Sbt(List("doc", "mdoc"),
                       name = Some("Documentation")
      )
    ),
    scalas = List(Scala212)
  )
)

lazy val root = Project("frameless", file("." + "frameless")).in(file("."))
  .aggregate(core, cats, dataset, ml, docs)
  .settings(framelessSettings: _*)
  .settings(noPublishSettings: _*)

lazy val core = project
  .settings(name := "frameless-core")
  .settings(framelessSettings: _*)
  .settings(publishSettings: _*)


lazy val cats = project
  .settings(name := "frameless-cats")
  .settings(framelessSettings: _*)
  .settings(publishSettings: _*)
  .settings(
    addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.0" cross CrossVersion.full),
    scalacOptions += "-Ypartial-unification"
  )
  .settings(libraryDependencies ++= Seq(
    "org.typelevel"    %% "cats-core"      % catsCoreVersion,
    "org.typelevel"    %% "cats-effect"    % catsEffectVersion,
    "org.typelevel"    %% "cats-mtl-core"  % catsMtlVersion,
    "org.typelevel"    %% "alleycats-core" % catsCoreVersion,
    "org.apache.spark" %% "spark-core"     % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql"      % sparkVersion % "provided"))
  .dependsOn(dataset % "test->test;compile->compile")

lazy val dataset = project
  .settings(name := "frameless-dataset")
  .settings(framelessSettings: _*)
  .settings(framelessTypedDatasetREPL: _*)
  .settings(publishSettings: _*)
  .settings(libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core"      % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql"       % sparkVersion % "provided",
    "net.ceedubs"      %% "irrec-regex-gen" % irrecVersion % Test
  ))
  .dependsOn(core % "test->test;compile->compile")

lazy val ml = project
  .settings(name := "frameless-ml")
  .settings(framelessSettings: _*)
  .settings(framelessTypedDatasetREPL: _*)
  .settings(publishSettings: _*)
  .settings(libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
    "org.apache.spark" %% "spark-mllib"  % sparkVersion % "provided"
  ))
  .dependsOn(
    core % "test->test;compile->compile",
    dataset % "test->test;compile->compile"
  )

lazy val docs = project
  .in(file("mdocs"))
  .settings(framelessSettings: _*)
  .settings(noPublishSettings: _*)
  .settings(scalacOptions --= Seq("-Xfatal-warnings", "-Ywarn-unused-import"))
  .enablePlugins(MdocPlugin)
  .settings(libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql"  % sparkVersion,
    "org.apache.spark" %% "spark-mllib"  % sparkVersion
  ))
  .settings(
    addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.0" cross CrossVersion.full),
    scalacOptions ++= Seq(
      "-Ypartial-unification",
      "-Ydelambdafy:inline"
    )
  )
  .dependsOn(dataset, cats, ml)

lazy val framelessSettings = Seq(
  organization := "org.typelevel",
  scalacOptions ++= Seq(
    "-Xlint:-missing-interpolator,-unused,_",
    "-target:jvm-1.8",
    "-deprecation",
    "-encoding", "UTF-8",
    "-feature",
    "-unchecked",
    "-Xfatal-warnings",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Ywarn-unused-import",
    "-Ywarn-value-discard",
    "-language:existentials",
    "-language:implicitConversions",
    "-language:higherKinds",
    "-Xfuture"
  ),
  licenses += ("Apache-2.0", url("http://opensource.org/licenses/Apache-2.0")),
  Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDF"),
  libraryDependencies ++= Seq(
    "com.chuusai" %% "shapeless" % shapeless,
    "org.scalatest" %% "scalatest" % scalatest % "test",
    "org.scalatestplus" %% "scalatestplus-scalacheck" % scalatestplus % "test",
    "org.scalacheck" %% "scalacheck" % scalacheck % "test"),
  Test / javaOptions ++= Seq("-Xmx1G", "-ea"),
  Test / fork := true,
  Test / parallelExecution := false
) ++ consoleSettings

lazy val consoleSettings = Seq(
  Compile / console / scalacOptions ~= {_.filterNot("-Ywarn-unused-import" == _)},
  Test / console / scalacOptions := (Compile / console / scalacOptions).value
)

lazy val framelessTypedDatasetREPL = Seq(
  initialize ~= { _ => // Color REPL
    val ansi = System.getProperty("sbt.log.noformat", "false") != "true"
    if (ansi) System.setProperty("scala.color", "true")
  },
  console / initialCommands :=
    """
      |import org.apache.spark.{SparkConf, SparkContext}
      |import org.apache.spark.sql.SparkSession
      |import frameless.functions.aggregate._
      |import frameless.syntax._
      |
      |val conf = new SparkConf().setMaster("local[*]").setAppName("frameless repl").set("spark.ui.enabled", "false")
      |implicit val spark = SparkSession.builder().config(conf).appName("REPL").getOrCreate()
      |
      |import spark.implicits._
      |
      |spark.sparkContext.setLogLevel("WARN")
      |
      |import frameless.TypedDataset
    """.stripMargin,
  console / cleanupCommands :=
    """
      |spark.stop()
    """.stripMargin
)

lazy val publishSettings = Seq(
  publishMavenStyle := true,
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some(Opts.resolver.sonatypeSnapshots)
    else
      Some(Opts.resolver.sonatypeStaging)
  },
  Test / publishArtifact := false,
  pomIncludeRepository := Function.const(false),
  pomExtra in Global := {
    <url>https://github.com/typelevel/frameless</url>
    <scm>
      <url>git@github.com:typelevel/frameless.git</url>
      <connection>scm:git:git@github.com:typelevel/frameless.git</connection>
    </scm>
    <developers>
      <developer>
        <id>OlivierBlanvillain</id>
        <name>Olivier Blanvillain</name>
        <url>https://github.com/OlivierBlanvillain/</url>
      </developer>
      <developer>
        <id>adelbertc</id>
        <name>Adelbert Chang</name>
        <url>https://github.com/adelbertc/</url>
      </developer>
      <developer>
        <id>imarios</id>
        <name>Marios Iliofotou</name>
        <url>https://github.com/imarios/</url>
      </developer>
      <developer>
        <id>kanterov</id>
        <name>Gleb Kanterov</name>
        <url>https://github.com/kanterov/</url>
      </developer>
      <developer>
        <id>non</id>
        <name>Erik Osheim</name>
        <url>https://github.com/non/</url>
      </developer>
      <developer>
        <id>jeremyrsmith</id>
        <name>Jeremy Smith</name>
        <url>https://github.com/jeremyrsmith/</url>
      </developer>
    </developers>
  }
)

lazy val noPublishSettings = Seq(
  publish := (()),
  publishLocal := (()),
  publishArtifact := false
)

lazy val credentialSettings = Seq(
  // For Travis CI - see http://www.cakesolutions.net/teamblogs/publishing-artefacts-to-oss-sonatype-nexus-using-sbt-and-travis-ci
  credentials ++= (for {
    username <- Option(System.getenv().get("SONATYPE_USERNAME"))
    password <- Option(System.getenv().get("SONATYPE_PASSWORD"))
  } yield Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", username, password)).toSeq
)


lazy val copyReadme = taskKey[Unit]("copy for website generation")
lazy val copyReadmeImpl = Def.task {
  val from = baseDirectory.value / "README.md"
  val to   = baseDirectory.value / "docs" / "src" / "main" / "tut" / "README.md"
  sbt.IO.copy(List((from, to)), overwrite = true, preserveLastModified = true, preserveExecutable = true)
}
copyReadme := copyReadmeImpl.value
