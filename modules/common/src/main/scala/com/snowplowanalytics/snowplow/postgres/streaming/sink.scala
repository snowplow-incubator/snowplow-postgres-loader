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

import cats.data.EitherT
import cats.implicits._

import cats.effect.{Clock, ContextShift, Sync}

import doobie._
import doobie.implicits._

import io.circe.Json
import org.log4s.getLogger

import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.iglu.client.Client

import com.snowplowanalytics.snowplow.badrows.{BadRow, Payload, Processor}
import com.snowplowanalytics.snowplow.postgres.api.{DB, State}
import com.snowplowanalytics.snowplow.postgres.shredding.{Entity, transform}
import com.snowplowanalytics.snowplow.postgres.streaming.data.{BadData, Data}
import com.snowplowanalytics.snowplow.postgres.logging.Slf4jLogHandler

object sink {

  private lazy val logger = getLogger
  private lazy val logHandler = Slf4jLogHandler(logger)

  type Insert = ConnectionIO[Unit]

  def sinkResult[F[_]: Sync: ContextShift: Clock: DB](
    state: State[F],
    client: Client[F, Json],
    processor: Processor
  )(result: Either[BadData, Data]): F[Unit] =
    result.fold(sinkBad[F], sinkGood[F](state, client, processor)(_).flatMap {
      case Left(badData) => sinkBad[F](badData)
      case Right(_) => Sync[F].unit
    })

  /**
    * Sink good events into Postgres. During sinking, payloads go through all transformation steps
    * and checking the state of the DB itself.
    * Events that could not be transformed (due Iglu errors or DB unavailability) are emitted as bad data
    * @param state mutable Loader state
    * @param client Iglu Client
    * @param processor The actor processing these events
    */
  def sinkGood[F[_]: Sync: Clock: DB](state: State[F], client: Client[F, Json], processor: Processor)(payload: Data): F[Either[BadData, Unit]] = {
    val result = for {
      entities <- payload match {
        case Data.Snowplow(event) =>
          transform.shredEvent[F](client, processor, event).leftMap(bad => BadData.BadEnriched(bad))
        case Data.SelfDescribing(json) =>
          transform.shredJson(client)(json).leftMap(errors => BadData.BadJson(json.normalize.noSpaces, errors.toString))
      }
      insert <- EitherT(DB.process(entities, state).attempt).leftMap {
        case error =>
          payload match {
            case Data.Snowplow(event) =>
              val badRow = BadRow.LoaderRuntimeError(processor, error.getMessage, Payload.LoaderPayload(event))
              BadData.BadEnriched(badRow)
            case Data.SelfDescribing(json) =>
              BadData.BadJson(json.normalize.noSpaces, s"Cannot insert: ${error.getMessage}")
          }
      }
    } yield insert
    result.value
  }
  
  def sinkBad[F[_]: Sync: ContextShift: Clock: DB](badData: BadData): F[Unit] =
    badData match {
      case BadData.BadEnriched(row) => Sync[F].delay(logger.warn(row.compact))
      case BadData.BadJson(payload, error) => Sync[F].delay(logger.warn(s"Cannot parse $payload. $error"))
    }

  /**
    * Build an `INSERT` action for a single entity
    * Multiple inserts later can be combined into a transaction
    */
  def insertStatement(schema: String, row: Entity): Insert = {
    val length = row.columns.length

    val columns = Fragment.const0(row.columns.map(c => s"""\"${c.name}\"""").mkString(","))

    val table = Fragment.const0(s"$schema.${row.tableName}")
    val values = row.columns.zipWithIndex.foldLeft(fr0"") {
      case (acc, (cur, i)) if i < length - 1 => acc ++ cur.value.fragment ++ fr0","
      case (acc, (cur, _))                   => acc ++ cur.value.fragment
    }

    fr"""INSERT INTO $table ($columns) VALUES ($values)""".update(logHandler).run.void
  }

}
