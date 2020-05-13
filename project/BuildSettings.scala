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

object BuildSettings {
  lazy val projectSettings = Seq(
    organization := "com.snowplowanalytics",
    name := "snowplow-postgres-loader",
    version := "0.1.0",
    scalaVersion := "2.12.11",
    description := "Loading Snowplow enriched data into PostgreSQL in real-time"
  )

  // Make package (build) metadata available within source code.
  lazy val scalifiedSettings = Seq(
    sourceGenerators in Compile += Def.task {
      val file = (sourceManaged in Compile).value / "settings.scala"
      IO.write(file, """package com.snowplowanalytics.snowplow.postgres.loader.generated
                       |object ProjectSettings {
                       |  val organization = "%s"
                       |  val name = "%s"
                       |  val version = "%s"
                       |  val scalaVersion = "%s"
                       |  val description = "%s"
                       |}
                       |""".stripMargin.format(organization.value, name.value, version.value, scalaVersion.value, description.value))
      Seq(file)
    }.taskValue
  )

  lazy val compilerSettings = Seq[Setting[_]](
    scalacOptions := Seq(
      "-deprecation",
      "-encoding", "UTF-8",
      "-explaintypes",
      "-feature",
      "-language:existentials",
      "-language:experimental.macros",
      "-language:higherKinds",
      "-language:implicitConversions",
      "-unchecked",
      "-Xcheckinit",
      "-Xfuture",
      "-Yno-adapted-args",
      "-Ypartial-unification",
      "-Ywarn-dead-code",
      "-Ywarn-extra-implicit",
      "-Ywarn-inaccessible",
      "-Ywarn-infer-any",
      "-Ywarn-nullary-override",
      "-Ywarn-nullary-unit",
      "-Ywarn-numeric-widen",
      "-Ywarn-unused",
      "-Ywarn-value-discard"
    ),
    javacOptions := Seq(
      "-source", "1.8",
      "-target", "1.8",
      "-Xlint"
    )
  )

  lazy val resolverSettings = Seq[Setting[_]](
    resolvers ++= Seq(
      "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"
    )
  )
}
