val sparkVersion = "3.2.1"
val spark31Version = "3.1.3"
val spark30Version = "3.0.3"
val catsCoreVersion = "2.7.0"
val catsEffectVersion = "3.3.7"
val catsMtlVersion = "1.2.0"
val scalatest = "3.2.11"
val scalatestplus = "3.1.0.0-RC2"
val shapeless = "2.3.7"
val scalacheck = "1.15.4"
val scalacheckEffect = "1.0.3"
val refinedVersion = "0.9.28"

val Scala212 = "2.12.15"
val Scala213 = "2.13.8"

ThisBuild / tlBaseVersion := "0.12"

ThisBuild / crossScalaVersions := Seq(Scala213, Scala212)
ThisBuild / scalaVersion := Scala212
ThisBuild / tlSkipIrrelevantScalas := true
ThisBuild / githubWorkflowArtifactUpload := false // doesn't work with scoverage

lazy val root = project.in(file("."))
  .enablePlugins(NoPublishPlugin)
  .aggregate(`root-spark32`, `root-spark31`, `root-spark30`, docs)

lazy val `root-spark32` = project
  .in(file(".spark32"))
  .enablePlugins(NoPublishPlugin)
  .aggregate(core, cats, dataset, refined, ml)

lazy val `root-spark31` = project
  .in(file(".spark31"))
  .enablePlugins(NoPublishPlugin)
  .aggregate(core, `cats-spark31`, `dataset-spark31`, `refined-spark31`, `ml-spark31`)

lazy val `root-spark30` = project
  .in(file(".spark30"))
  .enablePlugins(NoPublishPlugin)
  .aggregate(core, `cats-spark30`, `dataset-spark30`, `refined-spark30`, `ml-spark30`)

lazy val core = project
  .settings(name := "frameless-core")
  .settings(framelessSettings)

lazy val cats = project
  .settings(name := "frameless-cats")
  .settings(catsSettings)
  .dependsOn(dataset % "test->test;compile->compile;provided->provided")

lazy val `cats-spark31` = project
  .settings(name := "frameless-cats-spark31")
  .settings(sourceDirectory := (cats / sourceDirectory).value)
  .settings(catsSettings)
  .settings(spark31Settings)
  .dependsOn(`dataset-spark31` % "test->test;compile->compile;provided->provided")

lazy val `cats-spark30` = project
  .settings(name := "frameless-cats-spark30")
  .settings(sourceDirectory := (cats / sourceDirectory).value)
  .settings(catsSettings)
  .settings(spark30Settings)
  .dependsOn(`dataset-spark30` % "test->test;compile->compile;provided->provided")

lazy val dataset = project
  .settings(name := "frameless-dataset")
  .settings(datasetSettings)
  .settings(sparkDependencies(sparkVersion))
  .dependsOn(core % "test->test;compile->compile")

lazy val `dataset-spark31` = project
  .settings(name := "frameless-dataset-spark31")
  .settings(sourceDirectory := (dataset / sourceDirectory).value)
  .settings(datasetSettings)
  .settings(sparkDependencies(spark31Version))
  .settings(spark31Settings)
  .dependsOn(core % "test->test;compile->compile")

lazy val `dataset-spark30` = project
  .settings(name := "frameless-dataset-spark30")
  .settings(sourceDirectory := (dataset / sourceDirectory).value)
  .settings(datasetSettings)
  .settings(sparkDependencies(spark30Version))
  .settings(spark30Settings)
  .dependsOn(core % "test->test;compile->compile")

lazy val refined = project
  .settings(name := "frameless-refined")
  .settings(refinedSettings)
  .dependsOn(dataset % "test->test;compile->compile;provided->provided")

lazy val `refined-spark31` = project
  .settings(name := "frameless-refined-spark31")
  .settings(sourceDirectory := (refined / sourceDirectory).value)
  .settings(refinedSettings)
  .settings(spark31Settings)
  .dependsOn(`dataset-spark31` % "test->test;compile->compile;provided->provided")

lazy val `refined-spark30` = project
  .settings(name := "frameless-refined-spark30")
  .settings(sourceDirectory := (refined / sourceDirectory).value)
  .settings(refinedSettings)
  .settings(spark30Settings)
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
  .settings(sourceDirectory := (ml / sourceDirectory).value)
  .settings(mlSettings)
  .settings(sparkMlDependencies(spark31Version))
  .settings(spark31Settings)
  .dependsOn(
    core % "test->test;compile->compile",
    `dataset-spark31` % "test->test;compile->compile;provided->provided"
  )

lazy val `ml-spark30` = project
  .settings(name := "frameless-ml-spark30")
  .settings(sourceDirectory := (ml / sourceDirectory).value)
  .settings(mlSettings)
  .settings(sparkMlDependencies(spark30Version))
  .settings(spark30Settings)
  .dependsOn(
    core % "test->test;compile->compile",
    `dataset-spark30` % "test->test;compile->compile;provided->provided"
  )

lazy val docs = project
  .in(file("mdocs"))
  .settings(framelessSettings)
  .settings(scalacOptions --= Seq("-Xfatal-warnings", "-Ywarn-unused-import"))
  .enablePlugins(TypelevelSitePlugin)
  .settings(sparkDependencies(sparkVersion, Compile))
  .settings(sparkMlDependencies(sparkVersion, Compile))
  .settings(
    addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.2" cross CrossVersion.full),
    scalacOptions += "-Ydelambdafy:inline"
  )
  .dependsOn(dataset, cats, ml)

def sparkDependencies(sparkVersion: String, scope: Configuration = Provided) = Seq(
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % scope,
    "org.apache.spark" %% "spark-sql"  % sparkVersion % scope
  )
)

def sparkMlDependencies(sparkVersion: String, scope: Configuration = Provided) =
  Seq(libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion % scope)

lazy val catsSettings = framelessSettings ++ Seq(
  addCompilerPlugin("org.typelevel" % "kind-projector" % "0.13.2" cross CrossVersion.full),
  libraryDependencies ++= Seq(
    "org.typelevel" %% "cats-core"         % catsCoreVersion,
    "org.typelevel" %% "cats-effect"       % catsEffectVersion,
    "org.typelevel" %% "cats-mtl"          % catsMtlVersion,
    "org.typelevel" %% "alleycats-core"    % catsCoreVersion,
    "org.typelevel" %% "scalacheck-effect" % scalacheckEffect % Test
  )
)

lazy val datasetSettings = framelessSettings ++ framelessTypedDatasetREPL ++ Seq(
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

lazy val refinedSettings = framelessSettings ++ framelessTypedDatasetREPL ++ Seq(
  libraryDependencies += "eu.timepit" %% "refined" % refinedVersion
)

lazy val mlSettings = framelessSettings ++ framelessTypedDatasetREPL

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

lazy val scalacOptionSettings = Def.setting {
  def baseScalacOptions(scalaVersion: String) =
    CrossVersion.partialVersion(scalaVersion) match {
      case Some((2, 13)) => scalac213Options
      case _ => scalac212Options
    }

  baseScalacOptions(scalaVersion.value)
}

lazy val framelessSettings = Seq(
  scalacOptions ++= scalacOptionSettings.value,
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
  mimaPreviousArtifacts ~= {
    _.filterNot(_.revision == "0.11.0") // didn't release properly
  },
) ++ consoleSettings

lazy val spark30Settings = Seq(
  crossScalaVersions := Seq(Scala212)
)

lazy val spark31Settings = Seq(
  crossScalaVersions := Seq(Scala212)
)

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

ThisBuild / organization := "org.typelevel"
ThisBuild / licenses := List("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))
ThisBuild / developers := List(
  "OlivierBlanvillain" -> "Olivier Blanvillain",
  "adelbertc" -> "Adelbert Chang",
  "imarios" -> "Marios Iliofotou",
  "kanterov" -> "Gleb Kanterov",
  "non" -> "Erik Osheim",
  "jeremyrsmith" -> "Jeremy Smith",
  "cchantep" -> "Cédric Chantepie",
  "pomadchin" -> "Grigory Pomadchin"
).map { case (username, fullName) =>
  tlGitHubDev(username, fullName)
}

ThisBuild / tlCiReleaseBranches := Seq("master")
ThisBuild / tlSitePublishBranch := Some("master")

ThisBuild / githubWorkflowEnv += "SPARK_LOCAL_IP" -> "localhost"
ThisBuild / githubWorkflowBuildPreamble ++= Seq(
  WorkflowStep.Use(
    UseRef.Public("actions", "setup-python", "v2"),
    name = Some("Setup Python"),
    params = Map(
      "python-version" -> "3.x"
    )
  ),
  WorkflowStep.Run(
    List("pip install codecov"),
    name = Some("Setup codecov")
  )
)

val roots = List("root-spark30", "root-spark31", "root-spark32")
ThisBuild / githubWorkflowBuildMatrixAdditions +=
  "project" -> roots
ThisBuild / githubWorkflowArtifactDownloadExtraKeys += "project"
ThisBuild / githubWorkflowBuildSbtStepPreamble += s"project $${{ matrix.project }}"
ThisBuild / githubWorkflowBuildMatrixExclusions ++= roots.init.map { project =>
  MatrixExclude(Map("scala" -> Scala213, "project" -> project))
}

ThisBuild / githubWorkflowBuild ~= { steps =>
  steps.map { // replace the test step
    case _ @ WorkflowStep.Sbt(List("test"), _, _, _, _, _) =>
      WorkflowStep.Sbt(
        List("coverage", "test", "test/coverageReport"),
        name = Some("Test & Compute Coverage")
      )
    case step => step
  }
}

ThisBuild / githubWorkflowBuildPostamble ++= Seq(
  WorkflowStep.Run(
    List(s"codecov -F $${{ matrix.scala }}"),
    name = Some("Upload Codecov Results")
  )
)
