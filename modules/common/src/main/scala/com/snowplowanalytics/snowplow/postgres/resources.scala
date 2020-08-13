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

import cats.effect.{ContextShift, Async, Blocker, Clock, Resource, Concurrent, Sync}

import doobie.hikari._
import doobie.implicits._
import doobie.util.ExecutionContexts
import doobie.util.log.LogHandler
import doobie.util.transactor.Transactor

import io.circe.Json

import com.snowplowanalytics.iglu.client.Client

import com.snowplowanalytics.snowplow.postgres.api.State
import com.snowplowanalytics.snowplow.postgres.config.DBConfig
import com.snowplowanalytics.snowplow.postgres.config.DBConfig.JdbcUri

object resources {

  val FixedThreadPoolSize: Int = 32

  /** Initialise Blocking Thread Pool, Connection Pool, DB state and bad queue resources */
  def initialize[F[_]: Concurrent: Clock: ContextShift](postgres: DBConfig,
                                                        logger: LogHandler,
                                                        iglu: Client[F, Json]) =
    for {
      blocker <- Blocker[F]
      xa <- resources.getTransactor[F](postgres.getJdbc, postgres.username, postgres.password, blocker)
      state <- Resource.liftF(initializeState(postgres, logger, iglu, xa))
    } yield (blocker, xa, state)

  def initializeState[F[_]: Concurrent: Clock](postgres: DBConfig,
                            logger: LogHandler,
                            iglu: Client[F, Json],
                            xa: Transactor[F]): F[State[F]] = {
    for {
      ci <- storage.query.getComments(postgres.schema, logger).transact(xa).map(_.separate)
      (issues, comments) = ci
      _ <- issues.traverse_(issue => Sync[F].delay(println(issue)))
      initState = State.init[F](comments, iglu.resolver).value.flatMap {
        case Left(error) =>
          val exception = new RuntimeException(s"Couldn't initalise the state $error")
          Sync[F].raiseError[State[F]](exception)
        case Right(state) =>
          Sync[F].pure(state)
      }
      state <- initState
    } yield state
  }

  /** Get a HikariCP transactor */
  def getTransactor[F[_]: Async: ContextShift](jdbcUri: JdbcUri, user: String, password: String, be: Blocker): Resource[F, HikariTransactor[F]] =
    for {
      ce <- ExecutionContexts.fixedThreadPool[F](FixedThreadPoolSize)
      xa <- HikariTransactor.newHikariTransactor[F]("org.postgresql.Driver", jdbcUri.toString, user, password, ce, be)
    } yield xa

  /** Get default single-threaded transactor (use for tests only) */
  def getTransactorDefault[F[_]: Async: ContextShift](jdbcUri: JdbcUri, username: String, password: String): Transactor[F] =
    Transactor.fromDriverManager[F](
      "org.postgresql.Driver", jdbcUri.toString, username, password
    )
}
