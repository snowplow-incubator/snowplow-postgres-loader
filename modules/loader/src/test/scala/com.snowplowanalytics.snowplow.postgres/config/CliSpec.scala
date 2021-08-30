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

import java.nio.file.Paths

import scala.concurrent.duration._

import cats.effect.{IO, Clock}

import com.snowplowanalytics.snowplow.postgres.config.LoaderConfig.{InitPosition, Purpose, Source, Sink}
import com.snowplowanalytics.snowplow.postgres.config.LoaderConfig.{Monitoring, BackoffPolicy, PathInfo, PathType}

import software.amazon.awssdk.regions.Region

import blobstore.Path

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
          Source.Kinesis.Retrieval.FanOut,
          Source.Kinesis.CheckpointSettings(1000, 10.seconds)
        ),
        Sink(
          DBConfig(
            "localhost",
            5432,
            "snowplow",
            "postgres",
            "mysecretpassword",
            "REQUIRE",
            "atomic",
            10,
            None
          ),
          LoaderConfig.StreamSink.Noop
        ),
        Purpose.Enriched,
        Monitoring(Monitoring.Metrics(true)),
        BackoffPolicy(100.milliseconds, 10.seconds)
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
          "acme-postgres-loader",
          "enriched-events",
          Region.EU_CENTRAL_1,
          InitPosition.TrimHorizon,
          Source.Kinesis.Retrieval.FanOut,
          Source.Kinesis.CheckpointSettings(1000, 10.seconds)
        ),
        Sink(
          DBConfig(
            "localhost",
            5432,
            "snowplow",
            "postgres",
            "mysecretpassword",
            "REQUIRE",
            "atomic",
            10,
            Some(10)
          ),
          LoaderConfig.StreamSink.Kinesis(
            "bad-rows",
            Region.EU_CENTRAL_1,
            200.milliseconds,
            500L,
            5000000L
          )
        ),
        Purpose.Enriched,
        Monitoring(Monitoring.Metrics(true)),
        BackoffPolicy(100.milliseconds, 10.seconds)
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
          "my-subscription",
          Source.PubSub.CheckpointSettings(100)
        ),
        Sink(
          DBConfig(
            "localhost",
            5432,
            "snowplow",
            "postgres",
            "mysecretpassword",
            "REQUIRE",
            "atomic",
            10,
            None
          ),
          LoaderConfig.StreamSink.Noop
        ),
        Purpose.Enriched,
        Monitoring(Monitoring.Metrics(true)),
        BackoffPolicy(100.milliseconds, 10.seconds)
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
          "my-subscription",
          Source.PubSub.CheckpointSettings(100)
        ),
        Sink(
          DBConfig(
            "localhost",
            5432,
            "snowplow",
            "postgres",
            "mysecretpassword",
            "REQUIRE",
            "atomic",
            10,
            Some(10)
          ),
          LoaderConfig.StreamSink.PubSub(
            "my-project",
            "my-topic",
            200.milliseconds,
            500L,
            5000000L,
            1
          ),
        ),
        Purpose.Enriched,
        Monitoring(Monitoring.Metrics(true)),
        BackoffPolicy(100.milliseconds, 10.seconds)
      )
      val result = Cli.parse[IO](argv).value.unsafeRunSync()
      result must beRight.like {
        case Cli(config, _) => config must beEqualTo(expected)
      }
    }

    "accept example minimal local config" >> {
      val config = Paths.get(getClass.getResource("/config.local.minimal.hocon").toURI)
      val resolver = Paths.get(getClass.getResource("/resolver.json").toURI)
      val argv = List("--config", config.toString, "--resolver", resolver.toString)

      val expected = LoaderConfig(
        Source.Local(PathInfo(Path.fromString("/tmp/example").get, PathType.Absolute)),
        Sink(
          DBConfig(
            "localhost",
            5432,
            "snowplow",
            "postgres",
            "mysecretpassword",
            "REQUIRE",
            "atomic",
            10,
            None
          ),
          LoaderConfig.StreamSink.Noop
        ),
        Purpose.Enriched,
        Monitoring(Monitoring.Metrics(true)),
        BackoffPolicy(100.milliseconds, 10.seconds)
      )
      val result = Cli.parse[IO](argv).value.unsafeRunSync()
      result must beRight.like {
        case Cli(config, _) => config must beEqualTo(expected)
      }
    }

    "accept example extended local config" >> {
      val config = Paths.get(getClass.getResource("/config.local.reference.hocon").toURI)
      val resolver = Paths.get(getClass.getResource("/resolver.json").toURI)
      val argv = List("--config", config.toString, "--resolver", resolver.toString)

      val expected = LoaderConfig(
        Source.Local(PathInfo(Path.fromString("./tmp/example").get, PathType.Relative)),
        Sink(
          DBConfig(
            "localhost",
            5432,
            "snowplow",
            "postgres",
            "mysecretpassword",
            "REQUIRE",
            "atomic",
            10,
            Some(10)
          ),
          LoaderConfig.StreamSink.Local(PathInfo(Path.fromString("./tmp/bad").get, PathType.Relative))
        ),
        Purpose.Enriched,
        Monitoring(Monitoring.Metrics(true)),
        BackoffPolicy(100.milliseconds, 10.seconds)
      )
      val result = Cli.parse[IO](argv).value.unsafeRunSync()
      result must beRight.like {
        case Cli(config, _) => config must beEqualTo(expected)
      }
    }

    "accept local config with relative path" >> {
      val config = Paths.get(getClass.getResource("/config.local.relativetest.hocon").toURI)
      val resolver = Paths.get(getClass.getResource("/resolver.json").toURI)
      val argv = List("--config", config.toString, "--resolver", resolver.toString)

      val expected = LoaderConfig(
        Source.Local(PathInfo(Path.fromString("tmp/example").get, PathType.Relative)),
        Sink(
          DBConfig(
            "localhost",
            5432,
            "snowplow",
            "postgres",
            "mysecretpassword",
            "REQUIRE",
            "atomic",
            10,
            None
          ),
          LoaderConfig.StreamSink.Noop
        ),
        Purpose.Enriched,
        Monitoring(Monitoring.Metrics(true)),
        BackoffPolicy(100.milliseconds, 10.seconds)
      )
      val result = Cli.parse[IO](argv).value.unsafeRunSync()
      result must beRight.like {
        case Cli(config, _) => config must beEqualTo(expected)
      }
    }
  }

}
