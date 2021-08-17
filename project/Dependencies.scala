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
    val postgres     = "42.2.23"
    val commons      = "1.15"
    val logback      = "1.2.5"

    // Scala third-party
    val decline      = "2.1.0"
    val catsEffect   = "2.5.3"
    val circe        = "0.14.1"
    val circeConfig  = "0.8.0"
    val fs2Aws       = "3.1.1"
    val fs2PubSub    = "0.17.0"
    val doobie       = "0.13.4"
    val fs2          = "2.5.9"
    val log4s        = "1.10.0"
    val config       = "1.4.1"
    val fs2BlobCore  = "0.7.3"
    val catsRetry    = "2.1.1"

    val analyticsSdk = "2.1.0"
    val badRows      = "2.1.1"
    val schemaDdl    = "0.14.1"
    val igluClient   = "1.1.1"

    // Testing
    val specs2       = "4.9.4"
    val scalaCheck   = "1.14.3"
  }

  // Java
  val logback       = "ch.qos.logback" % "logback-classic"        % V.logback
  val config        = "com.typesafe"   % "config"                 % V.config

  // Snyk warnings
  val postgres      = "org.postgresql" % "postgresql"             % V.postgres
  val commons       = "commons-codec"  % "commons-codec"          % V.commons

  // Scala third-party
  val catsEffect    = "org.typelevel"            %% "cats-effect"            % V.catsEffect
  val decline       = "com.monovore"             %% "decline"                % V.decline
  val fs2           = "co.fs2"                   %% "fs2-core"               % V.fs2
  val fs2Io         = "co.fs2"                   %% "fs2-io"                 % V.fs2
  val circe         = "io.circe"                 %% "circe-core"             % V.circe
  val circeGeneric  = "io.circe"                 %% "circe-generic"          % V.circe
  val circeExtras   = "io.circe"                 %% "circe-generic-extras"   % V.circe
  val circeParser   = "io.circe"                 %% "circe-parser"           % V.circe
  val circeConfig   = "io.circe"                 %% "circe-config"           % V.circeConfig
  val circeLiteral  = "io.circe"                 %% "circe-literal"          % V.circe
  val fs2Aws        = "io.laserdisc"             %% "fs2-aws"                % V.fs2Aws
  val fs2PubSub     = "com.permutive"            %% "fs2-google-pubsub-grpc" % V.fs2PubSub
  val doobie        = "org.tpolecat"             %% "doobie-core"            % V.doobie
  val doobiePg      = "org.tpolecat"             %% "doobie-postgres"        % V.doobie
  val doobiePgCirce = "org.tpolecat"             %% "doobie-postgres-circe"  % V.doobie
  val doobieHikari  = "org.tpolecat"             %% "doobie-hikari"          % V.doobie
  val log4s         = "org.log4s"                %% "log4s"                  % V.log4s
  val fs2BlobCore   = "com.github.fs2-blobstore" %% "core"                   % V.fs2BlobCore
  val catsRetry     = "com.github.cb372"         %% "cats-retry"             % V.catsRetry

  // Scala first-party
  val analyticsSdk = "com.snowplowanalytics" %% "snowplow-scala-analytics-sdk" % V.analyticsSdk
  val badRows      = "com.snowplowanalytics" %% "snowplow-badrows"             % V.badRows
  val schemaDdl    = "com.snowplowanalytics" %% "schema-ddl"                   % V.schemaDdl
  val igluClient   = "com.snowplowanalytics" %% "iglu-scala-client"            % V.igluClient

  // Testing
  val specs2       = "org.specs2"     %% "specs2-core"       % V.specs2     % Test
  val specs2Check  = "org.specs2"     %% "specs2-scalacheck" % V.specs2     % Test
  val scalaCheck   = "org.scalacheck" %% "scalacheck"        % V.scalaCheck % Test

}
