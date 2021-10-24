val sparkVersion = "3.2.0"
val spark30Version = "3.0.1"
val spark31Version = "3.1.2"
val catsCoreVersion = "2.6.1"
val catsEffectVersion = "2.4.0"
val catsMtlVersion = "0.7.1"
val scalatest = "3.2.10"
val scalatestplus = "3.1.0.0-RC2"
val shapeless = "2.3.7"
val scalacheck = "1.15.4"
val refinedVersion = "0.9.27"

val Scala212 = "2.12.15"
val Scala213 = "2.13.6"

val previousVersion = "0.10.1"

ThisBuild / versionScheme := Some("semver-spec")

ThisBuild / crossScalaVersions := Seq(Scala212, Scala213)
ThisBuild / scalaVersion := Scala212

ThisBuild / mimaFailOnNoPrevious := false

lazy val root = Project("frameless", file("." + "frameless")).in(file("."))
  .aggregate(
    core,
    cats,
    `cats-spark31`,
    `cats-spark30`,
    dataset,
    `dataset-spark31`,
    `dataset-spark30`,
    refined,
    `refined-spark31`,
    `refined-spark30`,
    ml,
    `ml-spark31`,
    `ml-spark30`,
    docs
  )
  .settings(framelessSettings)
  .settings(noPublishSettings)
  .settings(mimaPreviousArtifacts := Set.empty)
  .settings(
    /** Not all Spark versions support Scala 2.13. These commands are launched for the supported subset of projects only. */
    commands ++= Seq(
      Command.command("frameless-test") { currentState =>
        CrossVersion.partialVersion(scalaVersion.value) match {
          case Some((2, 13)) =>
            val projects = "core" :: "dataset" :: "refined" :: "ml" :: Nil
            projects.map(_ + "/test") :::
            projects.map(_ + "/test/coverageReport") :::
            currentState
          case _ => "test" :: "coverageReport" :: currentState
        }
      },
      Command.command("frameless-mimaReportBinaryIssues") { currentState =>
        CrossVersion.partialVersion(scalaVersion.value) match {
          case Some((2, 13)) =>
            val projects = "core" :: "dataset" :: "refined" :: "ml" :: Nil
            projects.map(_ + "/mimaReportBinaryIssues") ::: currentState
          case _ => "mimaReportBinaryIssues" :: currentState
        }
      }
    )
  )

lazy val core = project
  .settings(name := "frameless-core")
  .settings(framelessSettings)
  .settings(publishSettings)

lazy val cats = project
  .settings(name := "frameless-cats")
  .settings(catsSettings)
  .dependsOn(dataset % "test->test;compile->compile;provided->provided")

lazy val `cats-spark31` = project
  .settings(name := "frameless-cats-spark31")
  .settings(catsSettings)
  .settings(mimaPreviousArtifacts := Set.empty)
  .dependsOn(`dataset-spark31` % "test->test;compile->compile;provided->provided")

lazy val `cats-spark30` = project
  .settings(name := "frameless-cats-spark30")
  .settings(catsSettings)
  .settings(mimaPreviousArtifacts := Set.empty)
  .dependsOn(`dataset-spark30` % "test->test;compile->compile;provided->provided")

lazy val dataset = project
  .settings(name := "frameless-dataset")
  .settings(datasetSettings)
  .settings(sparkDependencies(sparkVersion))
  .dependsOn(core % "test->test;compile->compile")

lazy val `dataset-spark31` = project
  .settings(name := "frameless-dataset-spark31")
  .settings(datasetSettings)
  .settings(sparkDependencies(spark31Version))
  .settings(mimaPreviousArtifacts := Set.empty)
  .dependsOn(core % "test->test;compile->compile")

lazy val `dataset-spark30` = project
  .settings(name := "frameless-dataset-spark30")
  .settings(datasetSettings)
  .settings(sparkDependencies(spark30Version))
  .settings(mimaPreviousArtifacts := Set.empty)
  .dependsOn(core % "test->test;compile->compile")

lazy val refined = project
  .settings(name := "frameless-refined")
  .settings(refinedSettings)
  .dependsOn(dataset % "test->test;compile->compile;provided->provided")

lazy val `refined-spark31` = project
  .settings(name := "frameless-refined-spark31")
  .settings(refinedSettings)
  .dependsOn(`dataset-spark31` % "test->test;compile->compile;provided->provided")

lazy val `refined-spark30` = project
  .settings(name := "frameless-refined-spark30")
  .settings(refinedSettings)
  .dependsOn(`dataset-spark30` % "test->test;compile->compile;provided->provided")

lazy val ml = project
  .settings(name := "frameless-ml")
  .settings(mlSettings)
  .settings(sparkMlDependencies(sparkVersion))
  .dependsOn(
    core % "test->test;compile->compile",
    dataset % "test->test;compile->compile;provided->provided"
  )

lazy val `ml-spark31` = project
  .settings(name := "frameless-ml-spark31")
  .settings(mlSettings)
  .settings(sparkMlDependencies(spark31Version))
  .settings(mimaPreviousArtifacts := Set.empty)
  .dependsOn(
    core % "test->test;compile->compile",
    `dataset-spark31` % "test->test;compile->compile;provided->provided"
  )

lazy val `ml-spark30` = project
  .settings(name := "frameless-ml-spark30")
  .settings(mlSettings)
  .settings(sparkMlDependencies(spark30Version))
  .settings(mimaPreviousArtifacts := Set.empty)
  .dependsOn(
    core % "test->test;compile->compile",
    `dataset-spark30` % "test->test;compile->compile;provided->provided"
  )

lazy val docs = project
  .in(file("mdocs"))
  .settings(framelessSettings)
  .settings(noPublishSettings)
  .settings(scalacOptions --= Seq("-Xfatal-warnings", "-Ywarn-unused-import"))
  .enablePlugins(MdocPlugin)
  .settings(sparkDependencies(sparkVersion, Compile))
  .settings(sparkMlDependencies(sparkVersion, Compile))
  .settings(
    addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.2" cross CrossVersion.full),
    scalacOptions += "-Ydelambdafy:inline"
  )
  .settings(mimaPreviousArtifacts := Set())
  .dependsOn(dataset, cats, ml)

def sparkDependencies(sparkVersion: String, scope: Configuration = Provided) = Seq(
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % scope,
    "org.apache.spark" %% "spark-sql"  % sparkVersion % scope
  )
)

def sparkMlDependencies(sparkVersion: String, scope: Configuration = Provided) =
  Seq(libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion % scope)

lazy val catsSettings = framelessSettings ++ publishSettings ++ Seq(
  addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.2" cross CrossVersion.full),
  libraryDependencies ++= Seq(
    "org.typelevel" %% "cats-core"      % catsCoreVersion,
    "org.typelevel" %% "cats-effect"    % catsEffectVersion,
    "org.typelevel" %% "cats-mtl-core"  % catsMtlVersion,
    "org.typelevel" %% "alleycats-core" % catsCoreVersion
  )
)

lazy val datasetSettings = framelessSettings ++ framelessTypedDatasetREPL ++ publishSettings ++ Seq(
  mimaBinaryIssueFilters ++= {
    import com.typesafe.tools.mima.core._

    val imt = ProblemFilters.exclude[IncompatibleMethTypeProblem](_)
    val mc = ProblemFilters.exclude[MissingClassProblem](_)
    val dmm = ProblemFilters.exclude[DirectMissingMethodProblem](_)

    // TODO: Remove have version bump
    Seq(
      imt("frameless.TypedEncoder.mapEncoder"),
      imt("frameless.TypedEncoder.arrayEncoder"),
      imt("frameless.RecordEncoderFields.deriveRecordCons"),
      imt("frameless.RecordEncoderFields.deriveRecordLast"),
      mc("frameless.functions.FramelessLit"),
      mc(f"frameless.functions.FramelessLit$$"),
      dmm("frameless.functions.package.litAggr")
    )
  }
)

lazy val refinedSettings = framelessSettings ++ framelessTypedDatasetREPL ++ publishSettings ++ Seq(
  mimaPreviousArtifacts := Set.empty,
  libraryDependencies += "eu.timepit" %% "refined" % refinedVersion
)

lazy val mlSettings = framelessSettings ++ framelessTypedDatasetREPL ++ publishSettings

lazy val scalac212Options = Seq(
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
  "-Xfuture",
  "-Ypartial-unification"
)

lazy val scalac213Options = {
  val exclusions = Set(
    "-Yno-adapted-args",
    "-Ywarn-unused-import",
    "-Xfuture",
    // type TraversableOnce in package scala is deprecated, symbol literal is deprecated; use Symbol("a") instead
    "-Xfatal-warnings",
    "-Ypartial-unification"
  )

  // https://github.com/scala/bug/issues/12072
  val options = Seq("-Xlint:-byname-implicit")
  scalac212Options.filter(s => !exclusions.contains(s)) ++ options
}

lazy val scalacOptionSettings = Def.task {
  def baseScalacOptions(scalaVersion: String) =
    CrossVersion.partialVersion(scalaVersion) match {
      case Some((2, 13)) => scalac213Options
      case _ => scalac212Options
    }

  baseScalacOptions(scalaVersion.value)
}

lazy val framelessSettings = Seq(
  organization := "org.typelevel",
  scalacOptions ++= scalacOptionSettings.value,
  licenses += ("Apache-2.0", url("http://opensource.org/licenses/Apache-2.0")),
  homepage := Some(url("https://typelevel.org/frameless")),
  Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDF"),
  libraryDependencies ++= Seq(
    "com.chuusai" %% "shapeless" % shapeless,
    "org.scalatest" %% "scalatest" % scalatest % Test,
    "org.scalatestplus" %% "scalatestplus-scalacheck" % scalatestplus % Test,
    "org.scalacheck" %% "scalacheck" % scalacheck % Test
  ),
  Test / javaOptions ++= Seq("-Xmx1G", "-ea"),
  Test / fork := true,
  Test / parallelExecution := false,
  mimaPreviousArtifacts := Def.setting { CrossVersion.partialVersion(scalaVersion.value) match {
    // TODO: remove once 2.13 artifacts published
    case Some((2, 13)) => Set.empty[ModuleID]
    case _             => Set("org.typelevel" %% name.value % previousVersion)
  } }.value
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
  Test / publishArtifact := false,
  pomExtra in Global := {
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

lazy val copyReadme = taskKey[Unit]("copy for website generation")
lazy val copyReadmeImpl = Def.task {
  val from = baseDirectory.value / "README.md"
  val to   = baseDirectory.value / "docs" / "src" / "main" / "tut" / "README.md"
  sbt.IO.copy(List((from, to)), overwrite = true, preserveLastModified = true, preserveExecutable = true)
}
copyReadme := copyReadmeImpl.value

ThisBuild / githubWorkflowArtifactUpload := false

ThisBuild / githubWorkflowBuild := Seq(
  WorkflowStep.Use(
    UseRef.Public("actions", "setup-python", "v2"),
    name = Some("Setup Python"),
    params = Map("python-version" -> "3.x")
  ),
  WorkflowStep.Run(
    List("pip install codecov"),
    name = Some("Setup codecov")
  ),
  WorkflowStep.Sbt(
    List("coverage", "frameless-test"),
    name = Some("Test & Compute Coverage")
  ),
  WorkflowStep.Run(
    List("codecov -F ${{ matrix.scala }}"),
    name = Some("Upload Codecov Results")
  )
)

ThisBuild / githubWorkflowBuild += WorkflowStep.Sbt(
  List("frameless-mimaReportBinaryIssues"),
  name = Some("Binary compatibility check")
)

ThisBuild / githubWorkflowPublishTargetBranches := Seq(
  RefPredicate.Equals(Ref.Branch("master")),
  RefPredicate.StartsWith(Ref.Tag("v"))
)

ThisBuild / githubWorkflowPublish := Seq(
  WorkflowStep.Sbt(
    List("ci-release"),
    name = Some("Publish artifacts"),
    env = Map(
      "PGP_PASSPHRASE"    -> "${{ secrets.PGP_PASSPHRASE }}",
      "PGP_SECRET"        -> "${{ secrets.PGP_SECRET }}",
      "SONATYPE_PASSWORD" -> "${{ secrets.SONATYPE_PASSWORD }}",
      "SONATYPE_USERNAME" -> "${{ secrets.SONATYPE_USERNAME }}"
    ),
    cond = Some("${{ env.SONATYPE_PASSWORD != '' && env.SONATYPE_USERNAME != '' }}")
  )
)

ThisBuild / githubWorkflowAddedJobs ++= Seq(
  WorkflowJob(
    "docs",
    "Documentation",
    githubWorkflowJobSetup.value.toList ::: List(
      WorkflowStep.Sbt(
        List("doc", "mdoc"),
        name = Some("Documentation")
      )
    ),
    scalas = List(Scala212)
  )
)
