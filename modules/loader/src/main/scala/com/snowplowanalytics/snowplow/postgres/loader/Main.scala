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

import com.snowplowanalytics.snowplow.badrows.Processor
import com.snowplowanalytics.snowplow.postgres.api.DB
import com.snowplowanalytics.snowplow.postgres.config.Cli
import com.snowplowanalytics.snowplow.postgres.config.LoaderConfig.Purpose
import com.snowplowanalytics.snowplow.postgres.generated.BuildInfo
import com.snowplowanalytics.snowplow.postgres.resources
import com.snowplowanalytics.snowplow.postgres.storage.utils
import com.snowplowanalytics.snowplow.postgres.streaming.{sink, source}

object Main extends IOApp {

  val processor = Processor(BuildInfo.name, BuildInfo.version)

  def run(args: List[String]): IO[ExitCode] =
    Cli.parse[IO](args).value.flatMap {
      case Right(Cli(appConfig, iglu, debug)) =>
        val logger = if (debug) LogHandler.jdkLogHandler else LogHandler.nop
        resources.initialize[IO](appConfig.getLoaderConfig, logger, iglu).use {
          case (blocker, xa, state) =>
            source.getSource[IO](blocker, appConfig.purpose, appConfig.source) match {
              case Right(dataStream) =>
                val meta = appConfig.purpose.snowplow
                implicit val db: DB[IO] = DB.interpreter[IO](iglu.resolver, xa, logger, appConfig.schema, meta)
                for {
                  _ <- appConfig.purpose match {
                    case Purpose.Enriched => utils.prepare[IO](appConfig.schema, xa, logger)
                    case Purpose.SelfDescribing => IO.unit
                  }
                  goodSink = sink.goodSink[IO](state, iglu, processor)
                  badSink = sink.badSink[IO]
                  s = dataStream.observeEither(badSink, goodSink.andThen(_.through(badSink)))

                  _ <- s.compile.drain
                } yield ExitCode.Success
              case Left(error) =>
                IO.delay(System.err.println(s"Source initialization error\n${error.getMessage}")).as(ExitCode.Error)
            }
        }

      case Left(error) =>
        IO.delay(System.err.println(s"Configuration initialization failure\n$error")).as(ExitCode.Error)
    }
}