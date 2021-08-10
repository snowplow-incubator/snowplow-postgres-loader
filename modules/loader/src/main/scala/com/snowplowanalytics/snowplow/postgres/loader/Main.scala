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
package com.snowplowanalytics.snowplow.postgres.loader

import cats.effect.{IOApp, IO, ExitCode}
import cats.implicits._

import org.log4s.getLogger

import doobie.hikari.HikariTransactor

import fs2.Pipe

import com.snowplowanalytics.snowplow.analytics.scalasdk.ParsingError.NotTSV
import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor, Payload}

import com.snowplowanalytics.snowplow.postgres.api.{State, DB}
import com.snowplowanalytics.snowplow.postgres.config.Cli
import com.snowplowanalytics.snowplow.postgres.config.LoaderConfig.{Purpose, Source}
import com.snowplowanalytics.snowplow.postgres.generated.BuildInfo
import com.snowplowanalytics.snowplow.postgres.resources
import com.snowplowanalytics.snowplow.postgres.storage.utils
import com.snowplowanalytics.snowplow.postgres.streaming.{sink, OrderedPipe}
import com.snowplowanalytics.snowplow.postgres.source.{PubsubEnv, KinesisEnv, Environment, LocalEnv}
import com.snowplowanalytics.snowplow.postgres.streaming.data.{Data, BadData}
import com.snowplowanalytics.snowplow.postgres.utils.ParserUtils

object Main extends IOApp {

  lazy val logger = getLogger

  val processor = Processor(BuildInfo.name, BuildInfo.version)

  def run(args: List[String]): IO[ExitCode] =
    Cli.parse[IO](args).flatMap(Cli.configPreCheck[IO]).value.flatMap {
      case Right(cli) => runWithCli(cli)
      case Left(error) => IO.delay(logger.error(s"Configuration initialization failure\n$error")).as(ExitCode.Error)
    }

  def runWithCli(cli: Cli[IO]): IO[ExitCode] = {
    val res = for {
      r <- resources.initialize[IO](cli.config.output, cli.iglu)
      (blocker, xa, state) = r
      env <- cli.config.input match {
        case config: Source.Kinesis => KinesisEnv.create[IO](blocker, config, cli.config.monitoring.metrics)
        case config: Source.PubSub => PubsubEnv.create[IO](blocker, config)
        case config: Source.LocalFS => LocalEnv.create[IO](blocker, config)
      }
    } yield (env, xa, state)
    res.use { case (env, xa, state) =>
      runWithEnv(env, cli, xa, state)
    }
  }

  private def runWithEnv[A](
    env: Environment[IO, A],
    cli: Cli[IO],
    xa: HikariTransactor[IO],
    state: State[IO]
  ): IO[ExitCode] = {
      val dataStream = env.source
        .map(record => (record, env.getPayload(record)))
        .map { case (record, payload) =>
          val res = payload.leftMap(createError(cli.config.purpose))
            .flatMap { p =>
              ParserUtils.parseItem(cli.config.purpose, p)
            }
          (record, res)
        }
      implicit val db: DB[IO] = DB.interpreter[IO](cli.iglu.resolver, xa, cli.config.output.schema)
      for {
        _ <- cli.config.purpose match {
          case Purpose.Enriched       => utils.prepare[IO](cli.config.output.schema, xa)
          case Purpose.SelfDescribing => IO.unit
        }
        s = dataStream.through(sinkAll(OrderedPipe.forTransactor(xa), sink.sinkResult(state, cli.iglu, processor)))
          .through(env.checkpointer)
        _ <- s.compile.drain
      } yield ExitCode.Success
  }

  private def sinkAll[A](orderedPipe: OrderedPipe[IO], sinkResult: Either[BadData, Data] => IO[Unit]): Pipe[IO, (A, Either[BadData, Data]), A] =
    orderedPipe {
      case (record, payload) => sinkResult(payload).map(_ => record)
    }

  private def createError(kind: Purpose)(payload: String): BadData =
    kind match {
      case Purpose.Enriched =>
        val badRow = BadRow.LoaderParsingError(Cli.processor, NotTSV, Payload.RawPayload(payload))
        BadData.BadEnriched(badRow)
      case Purpose.SelfDescribing =>
        BadData.BadJson(payload, "\"Cannot deserialize self-describing JSON from record\"")
    }

}
