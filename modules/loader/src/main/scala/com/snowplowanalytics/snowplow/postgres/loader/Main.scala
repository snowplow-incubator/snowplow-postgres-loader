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

import scala.concurrent.ExecutionContext

import cats.data.EitherT
import cats.effect.{IOApp, IO, ExitCode}

import org.log4s.getLogger

import doobie.hikari.HikariTransactor

import fs2.Pipe

import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor}
import com.snowplowanalytics.snowplow.postgres.api.{State, DB}
import com.snowplowanalytics.snowplow.postgres.config.Cli
import com.snowplowanalytics.snowplow.postgres.config.LoaderConfig.Purpose
import com.snowplowanalytics.snowplow.postgres.generated.BuildInfo
import com.snowplowanalytics.snowplow.postgres.resources
import com.snowplowanalytics.snowplow.postgres.storage.utils
import com.snowplowanalytics.snowplow.postgres.streaming.{Sink, SinkPipe}
import com.snowplowanalytics.snowplow.postgres.env.Environment
import com.snowplowanalytics.snowplow.postgres.streaming.data.Data
import com.snowplowanalytics.snowplow.postgres.utils.ParserUtils

object Main extends IOApp {

  lazy val logger = getLogger

  val processor = Processor(BuildInfo.name, BuildInfo.version)

  implicit val ec: ExecutionContext = executionContext

  def run(args: List[String]): IO[ExitCode] =
    Cli.parse[IO](args).flatMap(Cli.configPreCheck[IO]).value.flatMap {
      case Right(cli) => runWithCli(cli)
      case Left(error) => IO.delay(logger.error(s"Configuration initialization failure\n$error")).as(ExitCode.Error)
    }

  def runWithCli(cli: Cli[IO]): IO[ExitCode] = {
    val res = for {
      r <- resources.initialize[IO](cli.config.output.good, cli.iglu)
      (blocker, xa, state) = r
      env <- Environment.create[IO](cli.config, blocker)
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
      implicit val db: DB[IO] = DB.interpreter[IO](cli.iglu.resolver, xa, cli.config.output.good.schema)
      for {
        _ <- cli.config.purpose match {
          case Purpose.Enriched       => utils.prepare[IO](cli.config.output.good.schema, xa)
          case Purpose.SelfDescribing => IO.unit
        }
        _ <- env.source
          .through(parsePayload(env.getPayload, cli.config.purpose))
          .through(sinkAll(env.sinkPipe(xa), Sink.sinkResult(state, cli.iglu, processor, env.badRowSink)))
          .through(env.checkpointer)
          .compile
          .drain
      } yield ExitCode.Success
  }

  private def parsePayload[A](getPayload: A => IO[Either[BadRow, String]], purpose: Purpose): Pipe[IO, A, (A, Either[BadRow, Data])] =
    _.evalMap { record =>
      val p = for {
        payload <- EitherT(getPayload(record))
        parsedPayload <- EitherT(ParserUtils.parseItem[IO](purpose, payload))
      } yield parsedPayload
      p.value.map((record, _))
    }

  private def sinkAll[A](sinkPipe: SinkPipe[IO], sinkResult: Either[BadRow, Data] => IO[Unit]): Pipe[IO, (A, Either[BadRow, Data]), A] =
    sinkPipe {
      case (record, payload) => sinkResult(payload).map(_ => record)
    }
}
