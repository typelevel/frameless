package frameless

package object syntax extends FramelessSyntax {

  implicit val DefaultSparkDelay: SparkDelay[Job] =
    Job.framelessSparkDelayForJob
}
