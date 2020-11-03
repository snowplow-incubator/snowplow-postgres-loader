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

import bintray.BintrayPlugin._
import bintray.BintrayKeys._

import com.typesafe.sbt.SbtNativePackager.autoImport._
import com.typesafe.sbt.packager.linux.LinuxPlugin.autoImport._
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._

import scoverage.ScoverageKeys._

object BuildSettings {
  val scala212 = "2.12.11"
  val scala213 = "2.13.3"

  lazy val projectSettings = Seq(
    organization := "com.snowplowanalytics",
    version := "0.1.1-SNAPSHOT",
    scalaVersion := scala213,
    crossScalaVersions := Seq(scala212, scala213),
    description := "Loading Snowplow enriched data into PostgreSQL in real-time",
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0.html")),
    parallelExecution in Test := false
  )

  lazy val buildInfoSettings = Seq(
    buildInfoKeys := Seq[BuildInfoKey](name, version),
    buildInfoPackage := "com.snowplowanalytics.snowplow.postgres.generated"
  )

  /** Docker image settings */
  lazy val dockerSettings = Seq(
    maintainer in Docker := "Snowplow Analytics Ltd. <support@snowplowanalytics.com>",
    dockerBaseImage := "snowplow-docker-registry.bintray.io/snowplow/base-debian:0.2.1",
    daemonUser in Docker := "snowplow",
    dockerUpdateLatest := true,
    dockerRepository := Some("snowplow"),

    daemonUserUid in Docker := None,
    defaultLinuxInstallLocation in Docker := "/home/snowplow", // must be home directory of daemonUser
  )

  lazy val mavenSettings = bintraySettings ++ Seq(
    publishMavenStyle := true,
    publishArtifact := true,
    publishArtifact in Test := false,
    bintrayOrganization := Some("snowplow"),
    bintrayRepository := "snowplow-maven",
    pomIncludeRepository := { _ => false },
    homepage := Some(url("http://snowplowanalytics.com")),
    scmInfo := Some(ScmInfo(url("https://github.com/snowplow-incubator/snowplow-postgres-loader"),
      "scm:git@github.com:snowplow-incubator/snowplow-postgres-loader.git")),
    pomExtra := (
      <developers>
        <developer>
          <name>Snowplow Analytics Ltd</name>
          <email>support@snowplowanalytics.com</email>
          <organization>Snowplow Analytics Ltd</organization>
          <organizationUrl>http://snowplowanalytics.com</organizationUrl>
        </developer>
      </developers>)
  )

  lazy val scoverageSettings = Seq(
    coverageMinimum := 50,
    coverageFailOnMinimum := false,
    coverageExcludedPackages := "^target/.*",
    (test in Test) := {
      (coverageReport dependsOn (test in Test)).value
    }
  )

  lazy val addExampleConfToTestCp = Seq(
    unmanagedClasspath in Test += {
      baseDirectory.value.getParentFile.getParentFile / "config"
    }
  )
}
