import sbt.Keys._
import sbt._
import sbt.plugins.JvmPlugin

import org.scalafmt.sbt.ScalafmtPlugin.autoImport._

object Common extends AutoPlugin {
  override def trigger = allRequirements
  override def requires = JvmPlugin

  override def projectSettings = Seq(
    scalafmtFilter := "diff-ref=78f708d"
  )
}
