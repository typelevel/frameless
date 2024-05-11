ThisBuild / githubWorkflowArtifactUpload := false // doesn't work with scoverage

ThisBuild / githubWorkflowEnv += "SPARK_LOCAL_IP" -> "localhost"

ThisBuild / githubWorkflowArtifactDownloadExtraKeys += "project"

ThisBuild / githubWorkflowBuildSbtStepPreamble += s"project $${{ matrix.project }}"
ThisBuild / tlCiScalafmtCheck := true
ThisBuild / githubWorkflowBuild ~= { steps =>
  steps.map { // replace the test step
    case step: WorkflowStep.Sbt if step.commands == List("test") =>
      WorkflowStep.Sbt(
        commands = List("coverage", "test", "test/coverageReport"),
        name = Some("Test & Compute Coverage")
      )
    case step => step
  }
}

ThisBuild / githubWorkflowBuildPostamble +=
  WorkflowStep.Use(
    UseRef.Public(
      "codecov",
      "codecov-action",
      "v3"
    ),
    params = Map("flags" -> s"$${{ matrix.scala }}-$${{ matrix.project }}")
  )
