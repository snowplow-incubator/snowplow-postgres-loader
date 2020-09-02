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
package com.snowplowanalytics.snowplow.postgres.storage

import cats.Monad
import cats.implicits._

import cats.effect.Sync

import doobie.ConnectionIO
import doobie.implicits._
import doobie.util.transactor.Transactor
import doobie.util.log.LogHandler

import query.tableExists

object utils {

  def prepareEventsTable(schema: String, logger: LogHandler): ConnectionIO[Boolean] = {
    val create = ddl.createEventsTable(schema, logger).as(false)
    val exists = Monad[ConnectionIO].pure(true)
    Monad[ConnectionIO].ifM(tableExists(schema, "events", logger))(exists, create)
  }

  def prepare[F[_]: Sync](schema: String, xa: Transactor[F], logger: LogHandler): F[Unit] =
    prepareEventsTable(schema, logger).transact(xa).flatMap {
      case true  => Sync[F].delay(println(s"$schema.events table already exists"))
      case false => Sync[F].delay(println(s"$schema.events table created"))
    }
}
