logLevel := Level.Warn

addSbtPlugin("org.scalameta"    % "sbt-scalafmt"        % "2.4.0")
addSbtPlugin("org.scoverage"    % "sbt-scoverage"       % "1.6.1")
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.7.3")
addSbtPlugin("io.github.davidgregory084" % "sbt-tpolecat" % "0.1.13")
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.9.15")
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.9.0")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.9")
addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.2.7")
addSbtPlugin("com.geirsson" % "sbt-ci-release" % "1.5.7")
addSbtPlugin("com.dwijnand" % "sbt-dynver" % "4.1.1")
