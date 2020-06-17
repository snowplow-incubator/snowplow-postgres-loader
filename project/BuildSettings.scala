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

// sbt
import sbt._
import Keys._

import sbtbuildinfo.BuildInfoKey
import sbtbuildinfo.BuildInfoKeys._

import com.typesafe.sbt.SbtNativePackager.autoImport._
import com.typesafe.sbt.packager.linux.LinuxPlugin.autoImport._
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._

import scoverage.ScoverageKeys._

object BuildSettings {
  lazy val projectSettings = Seq(
    organization := "com.snowplowanalytics",
    name := "snowplow-postgres-loader",
    version := "0.1.0",
    scalaVersion := "2.13.2",
    description := "Loading Snowplow enriched data into PostgreSQL in real-time",
    parallelExecution in Test := false
  )

  lazy val buildInfoSettings = Seq(
    buildInfoKeys := Seq[BuildInfoKey](name, version),
    buildInfoPackage := "com.snowplowanalytics.snowplow.postgres.loader.generated"
  )

  /** Docker image settings */
  lazy val dockerSettings = Seq(
    maintainer in Docker := "Snowplow Analytics Ltd. <support@snowplowanalytics.com>",
    dockerBaseImage := "snowplow-docker-registry.bintray.io/snowplow/base-debian:0.1.0",
    daemonUser in Docker := "snowplow",
    dockerUpdateLatest := true,

    daemonUserUid in Docker := None,
    defaultLinuxInstallLocation in Docker := "/home/snowplow", // must be home directory of daemonUser
  )

  lazy val scoverageSettings = Seq(
    coverageMinimum := 50,
    coverageFailOnMinimum := false,
    (test in Test) := {
      (coverageReport dependsOn (test in Test)).value
    }
  )
}
