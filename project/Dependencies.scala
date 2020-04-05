/*
 * Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */

import sbt._

object Dependencies {

  object V {
    // Java
    val slf4j              = "1.7.25"

    // Scala third-party
    val decline      = "0.6.2"
    val catsEffect   = "2.1.2"
    val circe        = "0.13.1"
    val fs2Aws       = "2.28.37"
    val doobie       = "0.8.8"
    val analyticsSdk = "1.0.0"

    // Testing
    val specs2     = "4.7.0"
    val scalaCheck = "1.14.1"
  }

  // Java
  val logger       = "org.slf4j"             % "slf4j-simple"                            % V.slf4j

  // Scala third-party
  val decline      = "com.monovore" %% "decline" % V.decline
  val catsEffect   = "org.typelevel" %% "cats-effect" % V.catsEffect
  val circe        = List("circe-core", "circe-generic", "circe-parser", "circe-literal").map("io.circe" %% _ % V.circe)
  val fs2Aws       = "io.laserdisc" %% "fs2-aws" % V.fs2Aws
  val doobie       = "org.tpolecat" %% "doobie-core"      % V.doobie
  val analyticsSdk = "com.snowplowanalytics" %% "snowplow-scala-analytics-sdk" % V.analyticsSdk

  // Scala first-party

  // Testing
  val specs2     = "org.specs2"     %% "specs2-core" % V.specs2     % Test
  val scalaCheck = "org.scalacheck" %% "scalacheck"  % V.scalaCheck % Test

}
