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
  val scala213 = "2.13.9"

  lazy val projectSettings = Seq(
    organization := "com.snowplowanalytics",
    scalaVersion := scala213,
    crossScalaVersions := Seq(scala212, scala213),
    description := "Loading Snowplow enriched data into PostgreSQL in real-time",
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0.html")),
    Test / parallelExecution := false
  )

  lazy val buildInfoSettings = Seq(
    buildInfoKeys := Seq[BuildInfoKey](name, version),
    buildInfoPackage := "com.snowplowanalytics.snowplow.postgres.generated"
  )

  /** Docker image settings */
  lazy val dockerSettings = Seq(
    Docker / maintainer := "Snowplow Analytics Ltd. <support@snowplowanalytics.com>",
    dockerBaseImage := "adoptopenjdk:11-jre-hotspot-focal",
    Docker / daemonUser := "daemon",
    dockerUpdateLatest := true,
    dockerRepository := Some("snowplow"),

    Docker / daemonUserUid := None,
    Docker / defaultLinuxInstallLocation := "/opt/snowplow",
  )

  lazy val mavenSettings = Seq(
    publishArtifact := true,
    Test / publishArtifact := false,
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
    (Test / test) := {
      (coverageReport dependsOn (Test / test)).value
    }
  )

  lazy val addExampleConfToTestCp = Seq(
    Test / unmanagedClasspath += {
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

  /* Settings to ensure example config files can be resolved without error */
  lazy val testSettings = Seq(
    Test / fork := true,
    Test / envVars := Map("POSTGRES_PASSWORD" -> "mysecretpassword")
  )
}
