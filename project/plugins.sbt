val sbtTypelevelVersion = "0.4.22"

addSbtPlugin("org.typelevel" % "sbt-typelevel-ci-release" % sbtTypelevelVersion)

addSbtPlugin("org.typelevel" % "sbt-typelevel-site"       % sbtTypelevelVersion)

addSbtPlugin("org.scoverage" % "sbt-scoverage"            % "2.0.8")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.6")
