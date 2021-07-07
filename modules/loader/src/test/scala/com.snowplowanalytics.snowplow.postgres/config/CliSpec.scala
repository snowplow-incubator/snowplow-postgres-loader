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
package com.snowplowanalytics.snowplow.postgres.config

import java.nio.file.Paths

import cats.effect.{Clock, IO}

import com.snowplowanalytics.snowplow.postgres.config.LoaderConfig.{InitPosition, Monitoring, Purpose, Source}

import software.amazon.awssdk.regions.Region

import org.specs2.mutable.Specification

class CliSpec extends Specification {
  implicit val ioClock: Clock[IO] = Clock.create[IO]

  "Cli.parse" should {
    "accept example minimal kinesis config" >> {
      val config = Paths.get(getClass.getResource("/config.kinesis.minimal.hocon").toURI)
      val resolver = Paths.get(getClass.getResource("/resolver.json").toURI)
      val argv = List("--config", config.toString, "--resolver", resolver.toString)

      val expected = LoaderConfig(
        Source.Kinesis(
          "snowplow-postgres-loader",
          "enriched-events",
          Region.EU_CENTRAL_1,
          InitPosition.TrimHorizon,
          Source.Kinesis.Retrieval.FanOut
        ),
        DBConfig(
          "localhost",
          5432,
          "snowplow",
          "postgres",
          "mysecretpassword",
          "REQUIRE",
          "atomic"
        ),
        Purpose.Enriched,
        Monitoring(Monitoring.Metrics(true))
      )
      val result = Cli.parse[IO](argv).value.unsafeRunSync()
      result must beRight.like {
        case Cli(config, _) => config must beEqualTo(expected)
      }
    }

    "accept example extended kinesis config" >> {
      val config = Paths.get(getClass.getResource("/config.kinesis.reference.hocon").toURI)
      val resolver = Paths.get(getClass.getResource("/resolver.json").toURI)
      val argv = List("--config", config.toString, "--resolver", resolver.toString)

      val expected = LoaderConfig(
        Source.Kinesis(
          "snowplow-postgres-loader",
          "enriched-events",
          Region.EU_CENTRAL_1,
          InitPosition.TrimHorizon,
          Source.Kinesis.Retrieval.FanOut
        ),
        DBConfig(
          "localhost",
          5432,
          "snowplow",
          "postgres",
          "mysecretpassword",
          "REQUIRE",
          "atomic"
        ),
        Purpose.Enriched,
        Monitoring(Monitoring.Metrics(true))
      )
      val result = Cli.parse[IO](argv).value.unsafeRunSync()
      result must beRight.like {
        case Cli(config, _) => config must beEqualTo(expected)
      }
    }

    "accept example minimal pubsub config" >> {
      val config = Paths.get(getClass.getResource("/config.pubsub.minimal.hocon").toURI)
      val resolver = Paths.get(getClass.getResource("/resolver.json").toURI)
      val argv = List("--config", config.toString, "--resolver", resolver.toString)

      val expected = LoaderConfig(
        Source.PubSub(
          "my-project",
          "my-subscription"
        ),
        DBConfig(
          "localhost",
          5432,
          "snowplow",
          "postgres",
          "mysecretpassword",
          "REQUIRE",
          "atomic"
        ),
        Purpose.Enriched,
        Monitoring(Monitoring.Metrics(true))
      )
      val result = Cli.parse[IO](argv).value.unsafeRunSync()
      result must beRight.like {
        case Cli(config, _) => config must beEqualTo(expected)
      }
    }

    "accept example extended pubsub config" >> {
      val config = Paths.get(getClass.getResource("/config.pubsub.reference.hocon").toURI)
      val resolver = Paths.get(getClass.getResource("/resolver.json").toURI)
      val argv = List("--config", config.toString, "--resolver", resolver.toString)

      val expected = LoaderConfig(
        Source.PubSub(
          "my-project",
          "my-subscription"
        ),
        DBConfig(
          "localhost",
          5432,
          "snowplow",
          "postgres",
          "mysecretpassword",
          "REQUIRE",
          "atomic"
        ),
        Purpose.Enriched,
        Monitoring(Monitoring.Metrics(true))
      )
      val result = Cli.parse[IO](argv).value.unsafeRunSync()
      result must beRight.like {
        case Cli(config, _) => config must beEqualTo(expected)
      }
    }
  }

}
