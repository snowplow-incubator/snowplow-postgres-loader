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
package com.snowplowanalytics.snowplow.postgres.env

import scala.concurrent.ExecutionContext

import cats.effect.{Async, Blocker, ContextShift, Timer, ConcurrentEffect}

import doobie.hikari.HikariTransactor

import fs2.{Stream, Pipe}

import com.snowplowanalytics.snowplow.badrows.BadRow
import com.snowplowanalytics.snowplow.postgres.streaming.{SinkPipe, StreamSink, DummyStreamSink}
import com.snowplowanalytics.snowplow.postgres.config.LoaderConfig
import com.snowplowanalytics.snowplow.postgres.config.LoaderConfig.Source
import com.snowplowanalytics.snowplow.postgres.env.kinesis.{KinesisSink, KinesisEnv}
import com.snowplowanalytics.snowplow.postgres.env.local.{LocalSink, LocalEnv}
import com.snowplowanalytics.snowplow.postgres.env.pubsub.{PubSubSink, PubSubEnv}

final case class Environment[F[_], A](
  source: Stream[F, A],
  badRowSink: StreamSink[F],
  getPayload: A => F[Either[BadRow, String]],
  checkpointer: Pipe[F, A, Unit],
  sinkPipe: HikariTransactor[F] => SinkPipe[F]
)

object Environment {

  def create[F[_]: ConcurrentEffect: ContextShift: Timer](config: LoaderConfig,
                                                          blocker: Blocker) =
    for {
      badSink <- config.output.bad match {
        case LoaderConfig.StreamSink.Noop => DummyStreamSink.create
        case c: LoaderConfig.StreamSink.Kinesis => KinesisSink.create(c, config.monitoring, config.backoffPolicy, blocker)
        case c: LoaderConfig.StreamSink.PubSub => PubSubSink.create(c, config.backoffPolicy)
        case c: LoaderConfig.StreamSink.Local => LocalSink.create(c, blocker)
      }
      env <- config.input match {
        case c: Source.Kinesis => KinesisEnv.create[F](blocker, c, badSink, config.monitoring.metrics, config.purpose)
        case c: Source.PubSub => PubSubEnv.create[F](blocker, c, badSink)
        case c: Source.Local => LocalEnv.create[F](blocker, c, badSink)
      }
    } yield env

  def streamSinkExists[F[_]: Async: ContextShift](config: LoaderConfig.StreamSink)(implicit ec: ExecutionContext): F[Boolean] =
    config match {
      case c: LoaderConfig.StreamSink.Kinesis => KinesisSink.streamExists(c)
      case c: LoaderConfig.StreamSink.PubSub => PubSubSink.topicExists(c)
      case _ => Async[F].pure(true)
    }
}
