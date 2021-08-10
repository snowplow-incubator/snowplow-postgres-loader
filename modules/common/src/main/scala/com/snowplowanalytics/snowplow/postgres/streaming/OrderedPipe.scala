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

/** Evaluates effects, possibly concurrently, and emits the results downstream in any order
  */
trait OrderedPipe[F[_]] {
  def apply[A, B](f: A => F[B]): Pipe[F, A, B]
}

object OrderedPipe {

  /** An OrderedPipe in which results are emitted in the same order as the inputs
    *
   *  Use this OrderedPipe when a `Concurrent[F]` is not available
    */
  def sequential[F[_]]: OrderedPipe[F] =
    new OrderedPipe[F] {
      override def apply[A, B](f: A => F[B]): Pipe[F, A, B] =
        _.evalMap(f)
    }

  /** An OrderedPipe that evaluates effects in parallel.
    */
  def concurrent[F[_]: Concurrent](maxConcurrent: Int): OrderedPipe[F] =
    new OrderedPipe[F] {
      override def apply[A, B](f: A => F[B]): Pipe[F, A, B] =
        _.parEvalMap(maxConcurrent)(f)
    }

  /** A concurrent OrderedPipe whose parallelism matches the size of the transactor's underlying connection pool.
    *
   * Use this OrderedPipe whenever the effect requires a database connection
    */
  def forTransactor[F[_]: Concurrent](xa: HikariTransactor[F]): OrderedPipe[F] =
    concurrent(xa.kernel.getMaximumPoolSize)

}
