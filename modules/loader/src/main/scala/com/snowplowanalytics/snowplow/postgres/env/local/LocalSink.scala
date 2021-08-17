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

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption

import cats.effect.concurrent.Semaphore
import cats.effect.{ContextShift, Blocker, Resource, Concurrent}
import cats.implicits._

import com.snowplowanalytics.snowplow.postgres.streaming.StreamSink
import com.snowplowanalytics.snowplow.postgres.config.LoaderConfig

object LocalSink {

  def create[F[_]: Concurrent: ContextShift](config: LoaderConfig.StreamSink.Local, blocker: Blocker): Resource[F, StreamSink[F]] =
    for {
      channel <- Resource.fromAutoCloseableBlocking(blocker)(
        Concurrent[F].delay(FileChannel.open(config.path.allPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND))
      )
      sem <- Resource.eval(Semaphore(1L))
    } yield (payload: Array[Byte]) =>
      sem.withPermit {
        blocker.delay {
          channel.write(ByteBuffer.wrap(payload))
          channel.write(ByteBuffer.wrap(Array('\n'.toByte)))
        }.void
      }

}
