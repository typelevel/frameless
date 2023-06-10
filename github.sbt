ThisBuild / githubWorkflowArtifactUpload := false // doesn't work with scoverage

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

ThisBuild / githubWorkflowArtifactDownloadExtraKeys += "project"

ThisBuild / githubWorkflowBuildSbtStepPreamble += s"project $${{ matrix.project }}"

ThisBuild / githubWorkflowBuild ~= { steps =>
  steps.flatMap { // replace the test step
    case _ @WorkflowStep.Sbt(List("test"), _, _, _, _, _) =>
      List(
        WorkflowStep.Sbt(
          List("scalafmtSbtCheck", "scalafmtCheck"),
          name = Some("Check code style")
        ),
        WorkflowStep.Sbt(
          List("coverage", "test", "test/coverageReport"),
          name = Some("Test & Compute Coverage")
        )
      )

    case step => List(step)
  }
}

ThisBuild / githubWorkflowBuildPostamble ++= Seq(
  WorkflowStep.Run(
    List(s"codecov -F $${{ matrix.scala }}"),
    name = Some("Upload Codecov Results")
  )
)
