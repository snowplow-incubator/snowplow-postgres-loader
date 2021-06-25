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
package com.snowplowanalytics.snowplow.postgres.storage

import cats.Monad
import cats.implicits._

import cats.effect.Sync
import org.log4s.getLogger

import doobie.ConnectionIO
import doobie.implicits._
import doobie.util.transactor.Transactor

import query.tableExists
import definitions.EventsTableName

object utils {

  private lazy val logger = getLogger

  def prepareEventsTable(schema: String): ConnectionIO[Boolean] = {
    val create = ddl.createEventsTable(schema).as(false)
    val exists = Monad[ConnectionIO].pure(true)
    Monad[ConnectionIO].ifM(tableExists(schema, EventsTableName))(exists, create)
  }

  def prepare[F[_]: Sync](schema: String, xa: Transactor[F]): F[Unit] =
    prepareEventsTable(schema).transact(xa).flatMap {
      case true  => Sync[F].delay(logger.info(s"$schema.$EventsTableName table already exists"))
      case false => Sync[F].delay(logger.info(s"$schema.$EventsTableName table created"))
    }
}
