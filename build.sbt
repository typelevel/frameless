val sparkVersion = "3.5.9"
val spark40Version = "4.0.3"
val spark34Version = "3.4.4"
val catsCoreVersion = "2.13.0"
val catsEffectVersion = "3.7.0"
val catsMtlVersion = "1.7.0"
val scalatest = "3.2.20"
val scalatestplus = "3.1.0.0-RC2"
val shapeless = "2.3.13"
val scalacheck = "1.19.0"
val scalacheckEffect = "2.1.0"
val refinedVersion = "0.11.4"
val nakedFSVersion = "0.1.0"

val Scala212 = "2.12.21"
val Scala213 = "2.13.18"

ThisBuild / tlBaseVersion := "0.17"

ThisBuild / crossScalaVersions := Seq(Scala213, Scala212)
ThisBuild / scalaVersion := Scala213
ThisBuild / coverageScalacPluginVersion := "2.3.0"

lazy val root = project
  .in(file("."))
  .enablePlugins(NoPublishPlugin)
  .settings(crossScalaVersions := Nil)
  .aggregate(
    `root-spark40`,
    `root-spark35`,
    `root-spark34`,
    docs
  )

lazy val `root-spark40` = project
  .in(file(".spark40"))
  .enablePlugins(NoPublishPlugin)
  .settings(crossScalaVersions := Seq(Scala213))
  .aggregate(
    core,
    `cats-spark40`,
    `dataset-spark40`,
    `refined-spark40`,
    `ml-spark40`
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

lazy val `cats-spark40` = project
  .settings(name := "frameless-cats-spark40")
  .settings(sourceDirectory := (cats / sourceDirectory).value)
  .settings(catsSettings)
  .settings(spark40Settings)
  .dependsOn(
    `dataset-spark40` % "test->test;compile->compile;provided->provided"
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

lazy val `dataset-spark40` = project
  .settings(name := "frameless-dataset-spark40")
  .settings(sourceDirectory := (dataset / sourceDirectory).value)
  .settings(
    Compile / unmanagedSourceDirectories += (dataset / baseDirectory).value / "src" / "main" / "spark-4"
  )
  .settings(
    Test / unmanagedSourceDirectories += (dataset / baseDirectory).value / "src" / "test" / "spark-3.3+"
  )
  .settings(datasetSettings)
  .settings(sparkDependencies(spark40Version))
  .settings(spark40Settings)
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

lazy val `refined-spark40` = project
  .settings(name := "frameless-refined-spark40")
  .settings(sourceDirectory := (refined / sourceDirectory).value)
  .settings(refinedSettings)
  .settings(spark40Settings)
  .dependsOn(
    `dataset-spark40` % "test->test;compile->compile;provided->provided"
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
    `dataset-spark34` % "test->test;compile->compile;provided->provided"
  )

lazy val `ml-spark40` = project
  .settings(name := "frameless-ml-spark40")
  .settings(sourceDirectory := (ml / sourceDirectory).value)
  .settings(mlSettings)
  .settings(sparkMlDependencies(spark40Version))
  .settings(spark40Settings)
  .dependsOn(
    core % "test->test;compile->compile",
    `dataset-spark40` % "test->test;compile->compile;provided->provided"
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
      "org.typelevel" % "kind-projector" % "0.13.4" cross CrossVersion.full
    ),
    scalacOptions += "-Ydelambdafy:inline",
    libraryDependencies += "org.typelevel" %% "mouse" % "1.4.0",
    // mdoc executes Spark code via `Compile / runMain`; on JDK 17 (the site CI job) Spark
    // needs the module --add-opens flags, so fork the run and pass them through. Forking
    // changes the working directory, so pin it to the repo root where the docs read their
    // relative data files (e.g. docs/iris.data).
    Compile / run / fork := true,
    Compile / run / javaOptions ++= sparkJava17Options,
    Compile / run / baseDirectory := (LocalRootProject / baseDirectory).value
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
    "org.typelevel" % "kind-projector" % "0.13.4" cross CrossVersion.full
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
        dmm("org.apache.spark.sql.FramelessInternals.column"),
        // FramelessInternals is internal plumbing (Spark-version compat seam), not part of
        // the intended public API. Spark 4 required reworking it: `column` is now the
        // Expression->Column bridge and `mkDataset` derives the session from the source
        // Dataset instead of taking a SQLContext.
        imt("org.apache.spark.sql.FramelessInternals.column"),
        imt("org.apache.spark.sql.FramelessInternals.mkDataset")
      )
    },
    coverageExcludedPackages := "org.apache.spark.sql.reflection",
    libraryDependencies += "com.globalmentor" % "hadoop-bare-naked-local-fs" % nakedFSVersion % Test exclude (
      "org.apache.hadoop",
      "hadoop-commons"
    )
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

// JVM flags Spark needs on JDK 17+ (the module system blocks its reflective access
// to java.base internals otherwise). Empty on JDK 8/11. Reused by tests and the docs run.
lazy val sparkJava17Options: Seq[String] =
  if (sys.props("java.specification.version").toDouble >= 17.0) {
    Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    )
  } else Seq.empty

lazy val framelessSettings = Seq(
  scalacOptions ++= scalacOptionSettings.value,
  Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDF"),
  libraryDependencies ++= Seq(
    "com.chuusai" %% "shapeless" % shapeless,
    "org.scalatest" %% "scalatest" % scalatest % Test,
    "org.scalatestplus" %% "scalatestplus-scalacheck" % scalatestplus % Test,
    "org.scalacheck" %% "scalacheck" % scalacheck % Test
  ),
  Test / javaOptions ++= Seq("-Xmx1G", "-ea") ++ sparkJava17Options,
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
   * [error] 	* org.scala-lang.modules:scala-xml_2.12:2.3.0 (early-semver) is selected over 1.0.6
   * [error] 	    +- org.scoverage:scalac-scoverage-reporter_2.12:2.0.7 (depends on 2.4.0)
   * [error] 	    +- org.scala-lang:scala-compiler:2.12.16              (depends on 1.0.6)
   */
  libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
) ++ consoleSettings

lazy val spark40Settings = Seq[Setting[_]](
  // Spark 4 dropped Scala 2.12 support; this module is 2.13-only.
  crossScalaVersions := Seq(Scala213),
  scalaVersion := Scala213,
  tlVersionIntroduced := Map("2.13" -> "0.17.0"),
  // Brand-new artifact: no previously published version to check binary compatibility against.
  mimaPreviousArtifacts := Set.empty
)

lazy val spark34Settings = Seq[Setting[_]](
  tlVersionIntroduced := Map("2.12" -> "0.14.1", "2.13" -> "0.14.1"),
  mimaPreviousArtifacts := Set(
    organization.value %%
      moduleName.value
        .split("-")
        .dropRight(1)
        .mkString("-") % "0.14.1"
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

// Spark 3.x roots: 3.4 builds on 2.12 only, 3.5 builds on both 2.12 and 2.13.
val spark3Roots = List("root-spark34", "root-spark35")
// Spark 4.x roots: Scala 2.13 only (Spark 4 dropped 2.12).
val spark4Roots = List("root-spark40")
val roots = spark3Roots ++ spark4Roots

// Spark 3.x builds/tests on JDK 8; Spark 4 requires JDK 17+.
val spark3Java = JavaSpec.temurin("8")
val spark4Java = JavaSpec.temurin("17")

ThisBuild / githubWorkflowJavaVersions := Seq(spark3Java, spark4Java)

ThisBuild / githubWorkflowBuildMatrixAdditions += "project" -> roots

ThisBuild / githubWorkflowBuildMatrixExclusions ++=
  // 3.3/3.4 are 2.12-only; 3.5 builds both. Spark 4 is 2.13-only.
  spark3Roots.init.map { project =>
    MatrixExclude(Map("scala" -> "2.13", "project" -> project))
  } ++ spark4Roots.map { project =>
    MatrixExclude(Map("scala" -> "2.12", "project" -> project))
  } ++
    // Pin each Spark line to its JDK: 3.x on JDK 8, 4.x on JDK 17.
    spark3Roots.map { project =>
      MatrixExclude(Map("java" -> spark4Java.render, "project" -> project))
    } ++ spark4Roots.map { project =>
      MatrixExclude(Map("java" -> spark3Java.render, "project" -> project))
    }

ThisBuild / githubWorkflowEnv += "SBT_OPTS" -> "-Xms1g -Xmx4g"
