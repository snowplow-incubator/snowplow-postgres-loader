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

import sbtdynver.DynVerPlugin.autoImport._

import com.typesafe.sbt.SbtNativePackager.autoImport._
import com.typesafe.sbt.packager.linux.LinuxPlugin.autoImport._
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._

import scoverage.ScoverageKeys._

object BuildSettings {
  val scala212 = "2.12.11"
  val scala213 = "2.13.3"

  lazy val projectSettings = Seq(
    organization := "com.snowplowanalytics",
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
    dockerBaseImage := "snowplow/base-debian:0.2.1",
    daemonUser in Docker := "snowplow",
    dockerUpdateLatest := true,
    dockerRepository := Some("snowplow"),

    daemonUserUid in Docker := None,
    defaultLinuxInstallLocation in Docker := "/home/snowplow", // must be home directory of daemonUser
  )

  lazy val mavenSettings = Seq(
    publishArtifact := true,
    publishArtifact in Test := false,
    pomIncludeRepository := { _ => false },
    homepage := Some(url("http://snowplowanalytics.com")),
    developers := List(
      Developer(
        "Snowplow Analytics Ltd",
        "Snowplow Analytics Ltd",
        "support@snowplowanalytics.com",
        url("https://snowplowanalytics.com")
      )
    )
  )

  lazy val dynVerSettings = Seq(
    ThisBuild / dynverVTagPrefix := false, // Otherwise git tags required to have v-prefix
    ThisBuild / dynverSeparator := "-" // to be compatible with docker
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

  /** sbt-assembly settings for building a fat jar */
  import sbtassembly.AssemblyPlugin.autoImport._
  lazy val assemblySettings = Seq(
    assembly / assemblyJarName := { s"${moduleName.value}-${version.value}.jar" },
    assembly / assemblyMergeStrategy := {
      case "AUTHORS" => MergeStrategy.first
      case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.first
      case x if x.endsWith("native-image.properties") => MergeStrategy.first
      case x if x.endsWith("module-info.class") => MergeStrategy.first
      case x if x.endsWith("reflection-config.json") => MergeStrategy.first
      case x if x.startsWith("codegen-resources") => MergeStrategy.discard
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    }
  )
}
