val sparkVersion = "3.5.2"
val spark34Version = "3.4.3"
val spark33Version = "3.3.4"
val catsCoreVersion = "2.12.0"
val catsEffectVersion = "3.5.4"
val catsMtlVersion = "1.5.0"
val scalatest = "3.2.19"
val scalatestplus = "3.1.0.0-RC2"
val shapeless = "2.3.12"
val scalacheck = "1.18.0"
val scalacheckEffect = "1.0.4"
val refinedVersion = "0.11.2"
val nakedFSVersion = "0.1.0"

val Scala212 = "2.12.19"
val Scala213 = "2.13.15"

ThisBuild / tlBaseVersion := "0.16"

ThisBuild / crossScalaVersions := Seq(Scala213, Scala212)
ThisBuild / scalaVersion := Scala212

lazy val root = project
  .in(file("."))
  .enablePlugins(NoPublishPlugin)
  .settings(crossScalaVersions := Nil)
  .aggregate(
    `root-spark35`,
    `root-spark34`,
    `root-spark33`,
    docs
  )

lazy val `root-spark35` = project
  .in(file(".spark35"))
  .enablePlugins(NoPublishPlugin)
  .aggregate(core, cats, dataset, refined, ml)

lazy val `root-spark34` = project
  .in(file(".spark34"))
  .enablePlugins(NoPublishPlugin)
  .aggregate(
    core,
    `cats-spark34`,
    `dataset-spark34`,
    `refined-spark34`,
    `ml-spark34`
  )

lazy val `root-spark33` = project
  .in(file(".spark33"))
  .enablePlugins(NoPublishPlugin)
  .aggregate(
    core,
    `cats-spark33`,
    `dataset-spark33`,
    `refined-spark33`,
    `ml-spark33`
  )

lazy val core =
  project.settings(name := "frameless-core").settings(framelessSettings)

lazy val cats = project
  .settings(name := "frameless-cats")
  .settings(catsSettings)
  .dependsOn(dataset % "test->test;compile->compile;provided->provided")

lazy val `cats-spark34` = project
  .settings(name := "frameless-cats-spark34")
  .settings(sourceDirectory := (cats / sourceDirectory).value)
  .settings(catsSettings)
  .settings(spark34Settings)
  .dependsOn(
    `dataset-spark34` % "test->test;compile->compile;provided->provided"
  )

lazy val `cats-spark33` = project
  .settings(name := "frameless-cats-spark33")
  .settings(sourceDirectory := (cats / sourceDirectory).value)
  .settings(catsSettings)
  .settings(spark33Settings)
  .dependsOn(
    `dataset-spark33` % "test->test;compile->compile;provided->provided"
  )

lazy val dataset = project
  .settings(name := "frameless-dataset")
  .settings(
    Compile / unmanagedSourceDirectories += baseDirectory.value / "src" / "main" / "spark-3.4+"
  )
  .settings(
    Test / unmanagedSourceDirectories += baseDirectory.value / "src" / "test" / "spark-3.3+"
  )
  .settings(datasetSettings)
  .settings(sparkDependencies(sparkVersion))
  .dependsOn(core % "test->test;compile->compile")

lazy val `dataset-spark34` = project
  .settings(name := "frameless-dataset-spark34")
  .settings(sourceDirectory := (dataset / sourceDirectory).value)
  .settings(
    Compile / unmanagedSourceDirectories += (dataset / baseDirectory).value / "src" / "main" / "spark-3.4+"
  )
  .settings(
    Test / unmanagedSourceDirectories += (dataset / baseDirectory).value / "src" / "test" / "spark-3.3+"
  )
  .settings(datasetSettings)
  .settings(sparkDependencies(spark34Version))
  .settings(spark34Settings)
  .dependsOn(core % "test->test;compile->compile")

lazy val `dataset-spark33` = project
  .settings(name := "frameless-dataset-spark33")
  .settings(sourceDirectory := (dataset / sourceDirectory).value)
  .settings(
    Compile / unmanagedSourceDirectories += (dataset / baseDirectory).value / "src" / "main" / "spark-3"
  )
  .settings(
    Test / unmanagedSourceDirectories += (dataset / baseDirectory).value / "src" / "test" / "spark-3.3+"
  )
  .settings(datasetSettings)
  .settings(sparkDependencies(spark33Version))
  .settings(spark33Settings)
  .dependsOn(core % "test->test;compile->compile")

lazy val refined = project
  .settings(name := "frameless-refined")
  .settings(refinedSettings)
  .dependsOn(dataset % "test->test;compile->compile;provided->provided")

lazy val `refined-spark34` = project
  .settings(name := "frameless-refined-spark34")
  .settings(sourceDirectory := (refined / sourceDirectory).value)
  .settings(refinedSettings)
  .settings(spark34Settings)
  .dependsOn(
    `dataset-spark34` % "test->test;compile->compile;provided->provided"
  )

lazy val `refined-spark33` = project
  .settings(name := "frameless-refined-spark33")
  .settings(sourceDirectory := (refined / sourceDirectory).value)
  .settings(refinedSettings)
  .settings(spark33Settings)
  .dependsOn(
    `dataset-spark33` % "test->test;compile->compile;provided->provided"
  )

lazy val ml = project
  .settings(name := "frameless-ml")
  .settings(mlSettings)
  .settings(sparkMlDependencies(sparkVersion))
  .dependsOn(
    core % "test->test;compile->compile",
    dataset % "test->test;compile->compile;provided->provided"
  )

lazy val `ml-spark34` = project
  .settings(name := "frameless-ml-spark34")
  .settings(sourceDirectory := (ml / sourceDirectory).value)
  .settings(mlSettings)
  .settings(sparkMlDependencies(spark34Version))
  .settings(spark34Settings)
  .dependsOn(
    core % "test->test;compile->compile",
    `dataset-spark33` % "test->test;compile->compile;provided->provided"
  )

lazy val `ml-spark33` = project
  .settings(name := "frameless-ml-spark33")
  .settings(sourceDirectory := (ml / sourceDirectory).value)
  .settings(mlSettings)
  .settings(sparkMlDependencies(spark33Version))
  .settings(spark33Settings)
  .dependsOn(
    core % "test->test;compile->compile",
    `dataset-spark33` % "test->test;compile->compile;provided->provided"
  )

lazy val docs = project
  .in(file("mdocs"))
  .settings(framelessSettings)
  .settings(scalacOptions --= Seq("-Xfatal-warnings", "-Ywarn-unused-import"))
  .enablePlugins(TypelevelSitePlugin)
  .settings(sparkDependencies(sparkVersion, Compile))
  .settings(sparkMlDependencies(sparkVersion, Compile))
  .settings(
    addCompilerPlugin(
      "org.typelevel" % "kind-projector" % "0.13.3" cross CrossVersion.full
    ),
    scalacOptions += "-Ydelambdafy:inline",
    libraryDependencies += "org.typelevel" %% "mouse" % "1.3.2"
  )
  .dependsOn(dataset, cats, ml)

def sparkDependencies(
    sparkVersion: String,
    scope: Configuration = Provided
  ) = Seq(
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % scope,
    "org.apache.spark" %% "spark-sql" % sparkVersion % scope
  )
)

def sparkMlDependencies(sparkVersion: String, scope: Configuration = Provided) =
  Seq(
    libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion % scope
  )

lazy val catsSettings = framelessSettings ++ Seq(
  addCompilerPlugin(
    "org.typelevel" % "kind-projector" % "0.13.3" cross CrossVersion.full
  ),
  libraryDependencies ++= Seq(
    "org.typelevel" %% "cats-core" % catsCoreVersion,
    "org.typelevel" %% "cats-effect" % catsEffectVersion,
    "org.typelevel" %% "cats-mtl" % catsMtlVersion,
    "org.typelevel" %% "alleycats-core" % catsCoreVersion,
    "org.typelevel" %% "scalacheck-effect" % scalacheckEffect % Test
  )
)

lazy val datasetSettings =
  framelessSettings ++ framelessTypedDatasetREPL ++ Seq(
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
        dmm("frameless.functions.package.litAggr"),
        dmm("org.apache.spark.sql.FramelessInternals.column")
      )
    },
    coverageExcludedPackages := "org.apache.spark.sql.reflection",
    libraryDependencies += "com.globalmentor" % "hadoop-bare-naked-local-fs" % nakedFSVersion % Test exclude ("org.apache.hadoop", "hadoop-commons")
  )

lazy val refinedSettings =
  framelessSettings ++ framelessTypedDatasetREPL ++ Seq(
    libraryDependencies += "eu.timepit" %% "refined" % refinedVersion
  )

lazy val mlSettings = framelessSettings ++ framelessTypedDatasetREPL

lazy val scalac212Options = Seq(
  "-Xlint:-missing-interpolator,-unused,_",
  "-target:jvm-1.8",
  "-deprecation",
  "-encoding",
  "UTF-8",
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
      case _             => scalac212Options
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
  /**
   * The old Scala XML is pulled from Scala 2.12.x.
   *
   * [error] (update) found version conflict(s) in library dependencies; some are suspected to be binary incompatible:
   * [error]
   * [error] 	* org.scala-lang.modules:scala-xml_2.12:2.1.0 (early-semver) is selected over 1.0.6
   * [error] 	    +- org.scoverage:scalac-scoverage-reporter_2.12:2.0.7 (depends on 2.1.0)
   * [error] 	    +- org.scala-lang:scala-compiler:2.12.16              (depends on 1.0.6)
   */
  libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
) ++ consoleSettings

lazy val spark34Settings = Seq[Setting[_]](
  tlVersionIntroduced := Map("2.12" -> "0.14.1", "2.13" -> "0.14.1"),
  mimaPreviousArtifacts := Set(
    organization.value %% moduleName.value
      .split("-")
      .dropRight(1)
      .mkString("-") % "0.14.1"
  )
)

lazy val spark33Settings = Seq[Setting[_]](
  tlVersionIntroduced := Map("2.12" -> "0.13.0", "2.13" -> "0.13.0"),
  mimaPreviousArtifacts := Set(
    organization.value %% moduleName.value
      .split("-")
      .dropRight(1)
      .mkString("-") % "0.14.0"
  )
)

lazy val consoleSettings = Seq(
  Compile / console / scalacOptions ~= {
    _.filterNot("-Ywarn-unused-import" == _)
  },
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
ThisBuild / licenses := List(
  "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")
)
ThisBuild / developers := List(
  "OlivierBlanvillain" -> "Olivier Blanvillain",
  "adelbertc" -> "Adelbert Chang",
  "imarios" -> "Marios Iliofotou",
  "kanterov" -> "Gleb Kanterov",
  "non" -> "Erik Osheim",
  "jeremyrsmith" -> "Jeremy Smith",
  "cchantep" -> "Cédric Chantepie",
  "pomadchin" -> "Grigory Pomadchin"
).map {
  case (username, fullName) =>
    tlGitHubDev(username, fullName)
}

ThisBuild / tlCiReleaseBranches := Seq("master")
ThisBuild / tlSitePublishBranch := Some("master")

val roots = List("root-spark33", "root-spark34", "root-spark35")

ThisBuild / githubWorkflowBuildMatrixAdditions += "project" -> roots

ThisBuild / githubWorkflowBuildMatrixExclusions ++= roots.init.map { project =>
  MatrixExclude(Map("scala" -> "2.13", "project" -> project))
}

ThisBuild / githubWorkflowEnv += "SBT_OPTS" -> "-Xms1g -Xmx4g"
