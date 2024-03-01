val sbtTypelevelVersion = "0.6.6"

addSbtPlugin("org.typelevel" % "sbt-typelevel-ci-release" % sbtTypelevelVersion)

addSbtPlugin("org.typelevel" % "sbt-typelevel-site" % sbtTypelevelVersion)

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.11")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")

addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.6.4")
