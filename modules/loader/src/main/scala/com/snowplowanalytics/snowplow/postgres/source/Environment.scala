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
package com.snowplowanalytics.snowplow.postgres.source

import doobie.hikari.HikariTransactor

import fs2.{Stream, Pipe}

import com.snowplowanalytics.snowplow.postgres.streaming.SinkPipe
import com.snowplowanalytics.snowplow.postgres.streaming.data.BadData

final case class Environment[F[_], A](
  source: Stream[F, A],
  getPayload: A => Either[BadData, String],
  checkpointer: Pipe[F, A, Unit],
  sinkPipe: HikariTransactor[F] => SinkPipe[F]
)
