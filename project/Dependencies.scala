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
    val slf4j = "1.7.25"

    // Scala third-party
    val decline    = "1.2.0"
    val catsEffect = "2.1.2"
    val circe      = "0.13.0"
    val fs2Aws     = "2.28.37"
    val doobie     = "0.9.0"
    val fs2        = "2.3.0"
    val dhallj     = "0.3.1"

    val analyticsSdk = "1.0.0-M1"
    val badRows      = "2.0.0-M1"
    val schemaDdl    = "0.10.0"

    // Testing
    val specs2     = "4.7.0"
    val scalaCheck = "1.14.1"
  }

  // Java
  val logger = "org.slf4j" % "slf4j-simple" % V.slf4j

  // Scala third-party
  val decline       = "com.monovore"  %% "decline"           % V.decline
  val declineEffect = "com.monovore"  %% "decline-effect"    % V.decline
  val catsEffect    = "org.typelevel" %% "cats-effect"       % V.catsEffect
  val fs2           = "co.fs2"        %% "fs2-core"          % V.fs2
  val fs2Io         = "co.fs2"        %% "fs2-io"            % V.fs2
  val circe         = "io.circe"      %% "circe-core"        % V.circe
  val circeGeneric  = "io.circe"      %% "circe-generic"     % V.circe
  val circeParser   = "io.circe"      %% "circe-parser"      % V.circe
  val fs2Aws        = "io.laserdisc"  %% "fs2-aws"           % V.fs2Aws
  val doobie        = "org.tpolecat"  %% "doobie-core"       % V.doobie
  val doobiePg      = "org.tpolecat"  %% "doobie-postgres"   % V.doobie
  val dhall         = "org.dhallj"    %% "dhall-scala"       % V.dhallj
  val dhallCodec    = "org.dhallj"    %% "dhall-scala-codec" % V.dhallj
  val dhallCirce    = "org.dhallj"    %% "dhall-circe"       % V.dhallj

  // Scala first-party
  val analyticsSdk = "com.snowplowanalytics" %% "snowplow-scala-analytics-sdk" % V.analyticsSdk
  val badRows      = "com.snowplowanalytics" %% "snowplow-badrows"             % V.badRows
  val schemaDdl    = "com.snowplowanalytics" %% "schema-ddl"                   % V.schemaDdl

  // Testing
  val specs2     = "org.specs2"     %% "specs2-core" % V.specs2     % Test
  val scalaCheck = "org.scalacheck" %% "scalacheck"  % V.scalaCheck % Test

}
