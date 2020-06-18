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
package com.snowplowanalytics.snowplow.postgres.loader.config

import java.nio.file.Paths
import java.util.UUID

import cats.effect.{IO, Clock}

import com.snowplowanalytics.snowplow.postgres.loader.config.LoaderConfig.{Source, InitPosition, Purpose}

import org.specs2.mutable.Specification
import software.amazon.awssdk.regions.Region

class CliSpec extends Specification {
  implicit val ioClock: Clock[IO] = Clock.create[IO]

  "Cli.parse" should {
    "accept example config" >> {
      val config = Paths.get(getClass.getResource("/config.json").toURI)
      val resolver = Paths.get(getClass.getResource("/resolver.json").toURI)
      val argv = List("--config", config.toString, "--resolver", resolver.toString)

      val expected = LoaderConfig(
        "Acme Ltd. Snowplow Postgres",
        UUID.fromString("5c5e4353-4eeb-43da-98f8-2de6dc7fa947"),
        Source.Kinesis("acme-postgres-loader", "enriched-events", Region.EU_CENTRAL_1, InitPosition.TrimHorizon),
        "localhost",
        5432,
        "snowplow",
        "postgres",
        "mysecretpassword",
        "REQUIRE",
        "atomic",
        Purpose.Enriched
      )
      val result = Cli.parse[IO](argv).value.unsafeRunSync()
      result must beRight.like {
        case Cli(config, _, false) => config must beEqualTo(expected)
        case Cli(_, _, true) => ko("Unexpected debug flag")
      }
    }
  }

}
