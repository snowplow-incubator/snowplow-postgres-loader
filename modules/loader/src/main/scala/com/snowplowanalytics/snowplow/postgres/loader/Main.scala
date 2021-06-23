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

import org.log4s.getLogger

import com.snowplowanalytics.snowplow.badrows.Processor
import com.snowplowanalytics.snowplow.postgres.api.DB
import com.snowplowanalytics.snowplow.postgres.config.Cli
import com.snowplowanalytics.snowplow.postgres.config.LoaderConfig.Purpose
import com.snowplowanalytics.snowplow.postgres.generated.BuildInfo
import com.snowplowanalytics.snowplow.postgres.resources
import com.snowplowanalytics.snowplow.postgres.storage.utils
import com.snowplowanalytics.snowplow.postgres.streaming.{UnorderedPipe, sink, source}

object Main extends IOApp {

  lazy val logger = getLogger

  val processor = Processor(BuildInfo.name, BuildInfo.version)

  def run(args: List[String]): IO[ExitCode] =
    Cli.parse[IO](args).value.flatMap {
      case Right(Cli(loaderConfig, iglu)) =>
        resources.initialize[IO](loaderConfig.storage, iglu).use {
          case (blocker, xa, state) =>
            val dataStream = source.getSource[IO](blocker, loaderConfig.purpose, loaderConfig.input, loaderConfig.monitoring.metrics)
            implicit val db: DB[IO] = DB.interpreter[IO](iglu.resolver, xa, loaderConfig.storage.schema)
            for {
              _ <- loaderConfig.purpose match {
                case Purpose.Enriched       => utils.prepare[IO](loaderConfig.storage.schema, xa)
                case Purpose.SelfDescribing => IO.unit
              }
              badSink = sink.badSink[IO]
              goodSink = sink.goodSink[IO](UnorderedPipe.forTransactor(xa), state, iglu, processor).andThen(_.through(badSink))
              s = dataStream.observeEither(badSink, goodSink)

              _ <- s.compile.drain
            } yield ExitCode.Success
        }

      case Left(error) =>
        IO.delay(logger.error(s"Configuration initialization failure\n$error")).as(ExitCode.Error)
    }
}
