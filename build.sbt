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

lazy val root = project
  .in(file("."))
  .enablePlugins(JavaAppPackaging, DockerPlugin, BuildInfoPlugin)
  .settings(BuildSettings.projectSettings)
  .settings(BuildSettings.dockerSettings)
  .settings(BuildSettings.buildInfoSettings)
  .settings(
    resolvers += Dependencies.SnowplowBintray,
    libraryDependencies ++= Seq(
      Dependencies.logger,
      Dependencies.catsEffect,
      Dependencies.decline,
      Dependencies.circe,
      Dependencies.circeGeneric,
      Dependencies.circeExtras,
      Dependencies.circeParser,
      Dependencies.circeLiteral,
      Dependencies.doobie,
      Dependencies.doobiePg,
      Dependencies.doobiePgCirce,
      Dependencies.doobieHikari,
      Dependencies.fs2Aws,
      Dependencies.fs2PubSub,
      Dependencies.analyticsSdk,
      Dependencies.badRows,
      Dependencies.schemaDdl,
      Dependencies.specs2
    )
  )

addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
