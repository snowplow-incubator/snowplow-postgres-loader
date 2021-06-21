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
package com.snowplowanalytics.snowplow.postgres

import cats.implicits._

import cats.effect.{Async, Blocker, Clock, Concurrent, ContextShift, Resource, Sync}

import com.zaxxer.hikari.HikariConfig
import doobie.hikari._
import doobie.implicits._
import doobie.util.ExecutionContexts
import doobie.util.transactor.Transactor
import io.circe.Json
import org.log4s.getLogger

import com.snowplowanalytics.iglu.client.Client

import com.snowplowanalytics.snowplow.postgres.api.State
import com.snowplowanalytics.snowplow.postgres.config.DBConfig
import com.snowplowanalytics.snowplow.postgres.config.DBConfig.JdbcUri

object resources {

  private lazy val logger = getLogger

  /** Initialise Blocking Thread Pool, Connection Pool, DB state and bad queue resources */
  def initialize[F[_]: Concurrent: Clock: ContextShift](postgres: DBConfig, iglu: Client[F, Json]) =
    for {
      blocker <- Blocker[F]
      xa <- resources.getTransactor[F](DBConfig.hikariConfig(postgres), blocker)
      state <- Resource.eval(initializeState(postgres.schema, iglu, xa))
    } yield (blocker, xa, state)

  def initializeState[F[_]: Concurrent: Clock](schema: String, iglu: Client[F, Json], xa: HikariTransactor[F]): F[State[F]] =
    for {
      ci <- storage.query.getComments(schema).transact(xa).map(_.separate)
      (issues, comments) = ci
      _ <- issues.traverse_(issue => Sync[F].delay(logger.warn(issue.show)))
      initState = State.init[F](comments, iglu.resolver).value.flatMap {
        case Left(error) =>
          val exception = new RuntimeException(s"Couldn't initalise the state $error")
          Sync[F].raiseError[State[F]](exception)
        case Right(state) =>
          Sync[F].pure(state)
      }
      state <- initState
    } yield state

  /** Get a HikariCP transactor */
  def getTransactor[F[_]: Async: ContextShift](config: HikariConfig, be: Blocker): Resource[F, HikariTransactor[F]] = {
    val threadPoolSize = {
      // This could be made configurable, but these are sensible defaults and unlikely to be critical for tuning throughput.
      // Exceeding availableProcessors could lead to unnecessary context switching.
      // Exceeding the connection pool size is unnecessary, because that is limit of the app's parallelism.
      val maxPoolSize = if (config.getMaximumPoolSize > 0) config.getMaximumPoolSize else 10
      Math.min(maxPoolSize, Runtime.getRuntime.availableProcessors)
    }
    logger.debug(s"Using thread pool of size $threadPoolSize for Hikari transactor")

    for {
      ce <- ExecutionContexts.fixedThreadPool[F](threadPoolSize)
      xa <- HikariTransactor.fromHikariConfig[F](config, ce, be)
    } yield xa
  }

  /** Get default single-threaded transactor (use for tests only) */
  def getTransactorDefault[F[_]: Async: ContextShift](jdbcUri: JdbcUri, username: String, password: String): Transactor[F] =
    Transactor.fromDriverManager[F](
      "org.postgresql.Driver",
      jdbcUri.toString,
      username,
      password
    )
}
