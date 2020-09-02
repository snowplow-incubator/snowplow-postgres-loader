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
package com.snowplowanalytics.snowplow.postgres.streaming

import fs2.Pipe
import cats.effect.Concurrent
import doobie.hikari.HikariTransactor

/** Evaluates effects, possibly concurrently, and emits the results downstream in any order
  */
trait UnorderedPipe[F[_]] {
  def apply[A, B](f: A => F[B]): Pipe[F, A, B]
}

object UnorderedPipe {

  /** An UnorderedPipe in which results are emitted in the same order as the inputs
    *
   *  Use this UnorderedPipe when a `Concurrent[F]` is not available
    */
  def sequential[F[_]]: UnorderedPipe[F] =
    new UnorderedPipe[F] {
      override def apply[A, B](f: A => F[B]): Pipe[F, A, B] =
        _.evalMap(f)
    }

  /** An UnorderedPipe that evaluates effects in parallel.
    */
  def concurrent[F[_]: Concurrent](maxConcurrent: Int): UnorderedPipe[F] =
    new UnorderedPipe[F] {
      override def apply[A, B](f: A => F[B]): Pipe[F, A, B] =
        _.parEvalMapUnordered(maxConcurrent)(f)
    }

  /** A concurrent UnorderedPipe whose parallelism matches the size of the transactor's underlying connection pool.
    *
   * Use this UnorderedPipe whenever the effect requires a database connection
    */
  def forTransactor[F[_]: Concurrent](xa: HikariTransactor[F]): UnorderedPipe[F] =
    concurrent(xa.kernel.getMaximumPoolSize)

}
