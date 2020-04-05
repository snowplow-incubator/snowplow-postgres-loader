logLevel := Level.Warn

addSbtPlugin("org.scalameta"    % "sbt-scalafmt"        % "2.3.2")
addSbtPlugin("org.scoverage"    % "sbt-scoverage"       % "1.6.0")
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.4.1")
/* Uncomment if you need a fat jar, eg for AWS Lambda. */
//addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.10")
/* Uncomment to use for DynamoDB testing. */
//addSbtPlugin("com.localytics" % "sbt-dynamodb" % "2.0.3")
