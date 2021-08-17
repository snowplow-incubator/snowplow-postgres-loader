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

import fs2.Pipe
import cats.effect.Concurrent
import doobie.hikari.HikariTransactor

/**
  * Evaluates effects, possibly concurrently, and emits the results downstream
  */
sealed trait SinkPipe[F[_]] {
  def apply[A, B](f: A => F[B]): Pipe[F, A, B]
}

object SinkPipe {

  /** A pipe in which results are emitted in the same order as the inputs
    *
    *  Use this pipe when a `Concurrent[F]` is not available
    */
  def sequential[F[_]]: SinkPipe[F] =
    new SinkPipe[F] {
      override def apply[A, B](f: A => F[B]): Pipe[F, A, B] =
        _.evalMap(f)
    }

  object UnorderedPipe {

    /** An UnorderedPipe that evaluates effects in parallel.
      */
    def concurrent[F[_]: Concurrent](maxConcurrent: Int): SinkPipe[F] =
      new SinkPipe[F] {
        override def apply[A, B](f: A => F[B]): Pipe[F, A, B] =
          _.parEvalMapUnordered(maxConcurrent)(f)
      }

    /** A concurrent UnorderedPipe whose parallelism matches the size of the transactor's underlying connection pool.
      *
      * Use this UnorderedPipe whenever the effect requires a database connection
      */
    def forTransactor[F[_]: Concurrent](xa: HikariTransactor[F]): SinkPipe[F] =
      concurrent(xa.kernel.getMaximumPoolSize)

  }

  object OrderedPipe {

    /** An OrderedPipe that evaluates effects in parallel.
      */
    def concurrent[F[_]: Concurrent](maxConcurrent: Int): SinkPipe[F] =
      new SinkPipe[F] {
        override def apply[A, B](f: A => F[B]): Pipe[F, A, B] =
          _.parEvalMap(maxConcurrent)(f)
      }

    /** A concurrent OrderedPipe whose parallelism matches the size of the transactor's underlying connection pool.
      *
      * Use this OrderedPipe whenever the effect requires a database connection
      */
    def forTransactor[F[_]: Concurrent](xa: HikariTransactor[F]): SinkPipe[F] =
      concurrent(xa.kernel.getMaximumPoolSize)

  }

}
