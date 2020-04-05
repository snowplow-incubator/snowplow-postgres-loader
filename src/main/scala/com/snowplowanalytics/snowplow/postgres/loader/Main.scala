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
package com.snowplowanalytics.snowplow.postgres.loader

import cats.effect.{ExitCode, IO, IOApp}

import doobie.util.log.LogHandler

import com.snowplowanalytics.snowplow.postgres.loader.config.Cli
import com.snowplowanalytics.snowplow.postgres.loader.config.LoaderConfig.Purpose
import com.snowplowanalytics.snowplow.postgres.loader.storage.utils
import com.snowplowanalytics.snowplow.postgres.loader.streaming.{sink, source}

object Main extends IOApp {

  def run(args: List[String]): IO[ExitCode] =
    Cli.parse[IO](args).value.flatMap {
      case Right(Cli(postgres, iglu, debug)) =>
        val logger = if (debug) LogHandler.jdkLogHandler else LogHandler.nop
        resources.initialize[IO](postgres, logger, iglu).use {
          case (blocker, xa, state, badQueue) =>
            source.getSource[IO](blocker, postgres.purpose, postgres.source) match {
              case Right(dataStream) =>
                for {
                  _ <- postgres.purpose match {
                    case Purpose.Enriched => utils.prepare[IO](postgres.schema, xa, logger)
                    case Purpose.SelfDescribing => IO.unit
                  }
                  goodSink = sink.goodSink[IO](xa, logger, postgres.schema, state, badQueue, iglu)
                  badSink = sink.badSink[IO](badQueue)
                  _ <- dataStream.observeEither(badSink, goodSink).compile.drain
                } yield ExitCode.Success
              case Left(error) =>
                IO.delay(System.err.println(s"Source initialization error\n${error.getMessage}")).as(ExitCode.Error)
            }
        }

      case Left(error) =>
        IO.delay(System.err.println(s"Configuration initialization failure\n$error")).as(ExitCode.Error)
    }
}
