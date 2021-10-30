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
package com.snowplowanalytics.snowplow.postgres.streaming

import org.log4s.getLogger

import cats.effect.{Resource, Concurrent, Sync, Timer}
import cats.effect.concurrent.Ref

import fs2.Stream

import scala.concurrent.duration.FiniteDuration

object DummyStreamSink {
  def create[F[_]: Concurrent: Timer](period: FiniteDuration): Resource[F, StreamSink[F]] =
    for {
      counter <- Resource.eval(Ref.of(0))
      _ <- Concurrent[F].background(reporter(counter, period))
  } yield { _ =>
    counter.update(_ + 1)
  }

  lazy val logger = getLogger

  private def reporter[F[_]: Sync: Timer](counter: Ref[F, Int], period: FiniteDuration): F[Unit] =
    Stream.awakeDelay[F](period)
      .evalMap(_ => counter.getAndSet(0))
      .evalMap { count =>
        if (count > 0) Sync[F].delay(logger.info(s"Discarded $count bad rows during the last $period"))
        else Sync[F].unit
      }
      .compile
      .drain
}
