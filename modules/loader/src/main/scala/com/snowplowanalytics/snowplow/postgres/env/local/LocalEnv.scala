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
package com.snowplowanalytics.snowplow.postgres.env.local

import cats.implicits._
import cats.effect.{ContextShift, Blocker, Resource, Timer, ConcurrentEffect}

import fs2.{Pipe, Stream}

import blobstore.fs.FileStore
import blobstore.Store

import com.snowplowanalytics.snowplow.badrows.BadRow
import com.snowplowanalytics.snowplow.postgres.streaming.{SinkPipe, StreamSink}
import com.snowplowanalytics.snowplow.postgres.config.LoaderConfig.{Source}
import com.snowplowanalytics.snowplow.postgres.env.Environment

object LocalEnv {

  def create[F[_]: ConcurrentEffect : ContextShift : Timer](blocker: Blocker, config: Source.Local, badSink: StreamSink[F]): Resource[F, Environment[F, String]] = {
    Resource.eval {
      ConcurrentEffect[F].delay(
        Environment[F, String](
          getSource(blocker, config),
          badSink,
          getPayload,
          checkpointer,
          SinkPipe.UnorderedPipe.forTransactor[F]
        )
      )
    }
  }

  private def getSource[F[_]: ConcurrentEffect : ContextShift : Timer](blocker: Blocker, config: Source.Local): Stream[F, String] = {
    val store: Store[F] = FileStore[F](config.path.pathType.fsroot, blocker)
    store.list(config.path.value, recursive = true)
      .flatMap { p =>
        store.get(p, chunkSize = 32 * 1024)
          .through(fs2.text.utf8Decode)
          .through(fs2.text.lines)
          .filter(_.nonEmpty)
      }
  }

  private def getPayload[F[_]](record: String): Either[BadRow, String] = record.asRight

  private def checkpointer[F[_]]: Pipe[F, String, Unit] = _.map(_ => ())
}
