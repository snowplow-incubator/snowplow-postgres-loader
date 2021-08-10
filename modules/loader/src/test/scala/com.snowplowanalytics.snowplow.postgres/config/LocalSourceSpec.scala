/*
 * Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.postgres.config

import java.net.URI

import cats.effect.{IO, ExitCode}

import io.circe.Json

import blobstore.Path

import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.Registry
import com.snowplowanalytics.iglu.client.resolver.registries.Registry.{HttpConnection, Config, Http}
import com.snowplowanalytics.iglu.client.validator.CirceValidator

import com.snowplowanalytics.snowplow.postgres.Database
import com.snowplowanalytics.snowplow.postgres.config.LoaderConfig.{Purpose, Source, Monitoring}
import com.snowplowanalytics.snowplow.postgres.loader.Main

class LocalSourceSpec extends Database {
  import Database._

  "local event source" should {
    "work with given test events file" >> {

      val path = getClass.getResource("/test-events").getPath

      val registry = Http(Config("localhost registry", 1, Nil), HttpConnection(URI.create("http://localhost:8080/api/"), None))
      val igluClient = Client[IO, Json](Resolver(List(Registry.IgluCentral, registry), None), CirceValidator)

      val loaderConfig = LoaderConfig(
        Source.LocalFS(Source.LocalFS.PathInfo(Path.fromString(path).get, Source.LocalFS.PathType.Absolute)),
        DBConfig(
          "localhost",
          5432,
          "snowplow",
          "postgres",
          "mysecretpassword",
          "allow",
          "public"
        ),
        Purpose.Enriched,
        Monitoring(Monitoring.Metrics(false))
      )

      val cli = Cli[IO](loaderConfig, igluClient)

      val exitStatus = Main.runWithCli(cli).unsafeRunSync()

      val countCheckResult = for {
        b1 <- count("events").action.map(_ == 5)
        b2 <- count("com_snowplowanalytics_snowplow_geolocation_context_1").action.map(_ == 5)
        b3 <- count("com_snowplowanalytics_snowplow_ua_parser_context_1").action.map(_ == 5)
        b4 <- count("nl_basjes_yauaa_context_1").action.map(_ == 5)
        b5 <- count("org_w3_performance_timing_1").action.map(_ == 3)
      } yield b1 && b2 && b3 && b4 && b5

      countCheckResult.value.unsafeRunSync() must beRight(true)
      exitStatus mustEqual ExitCode(0)
    }
  }
}
