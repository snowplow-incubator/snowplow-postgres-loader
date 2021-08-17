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

import java.nio.charset.StandardCharsets

import cats.data.{EitherT, NonEmptyList}
import cats.implicits._

import cats.effect.{Clock, ContextShift, Sync}

import doobie._
import doobie.implicits._

import io.circe.Json
import io.circe.syntax._
import org.log4s.getLogger

import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.iglu.client.Client

import com.snowplowanalytics.snowplow.badrows.{BadRow, Payload, Processor, Failure}
import com.snowplowanalytics.snowplow.postgres.api.{DB, State}
import com.snowplowanalytics.snowplow.postgres.shredding.{Entity, transform}
import com.snowplowanalytics.snowplow.postgres.streaming.data.Data
import com.snowplowanalytics.snowplow.postgres.logging.Slf4jLogHandler

object Sink {

  private lazy val logger = getLogger
  private lazy val logHandler = Slf4jLogHandler(logger)

  type Insert = ConnectionIO[Unit]

  def sinkResult[F[_]: Sync: ContextShift: Clock: DB](
    state: State[F],
    client: Client[F, Json],
    processor: Processor,
    badSink: StreamSink[F]
  )(result: Either[BadRow, Data]): F[Unit] =
    result.fold(bad[F](_, badSink), good[F](state, client, processor)(_).flatMap {
      case Left(badRow) => bad[F](badRow, badSink)
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
  def good[F[_]: Sync: Clock: DB](state: State[F], client: Client[F, Json], processor: Processor)(payload: Data): F[Either[BadRow, Unit]] = {
    val result = for {
      now <- EitherT.liftF(TimeUtils.now)
      entities <- payload match {
        case Data.Snowplow(event) =>
          transform.shredEvent[F](client, processor, event)
        case Data.SelfDescribing(json) =>
          transform.shredJson(client)(json).leftMap { errors =>
            val failure = Failure.GenericFailure(
              now,
              errors.map(_.asJson.noSpaces)
            )
            BadRow.GenericError(processor, failure, Payload.RawPayload(json.normalize.noSpaces)).asInstanceOf[BadRow]
          }
      }
      insert <- EitherT(DB.process(entities, state).attempt).leftMap { error =>
          payload match {
            case Data.Snowplow(event) =>
              BadRow.LoaderRuntimeError(processor, error.getMessage, Payload.LoaderPayload(event)).asInstanceOf[BadRow]
            case Data.SelfDescribing(json) =>
              val failure = Failure.GenericFailure(
                now,
                NonEmptyList.of(s"Cannot insert: ${error.getMessage}")
              )
              BadRow.GenericError(processor, failure, Payload.RawPayload(json.normalize.noSpaces)).asInstanceOf[BadRow]
          }
      }
    } yield insert
    result.value
  }

  def bad[F[_]: Sync](badRow: BadRow, badSink: StreamSink[F]): F[Unit] =
    badSink(badRow.compact.getBytes(StandardCharsets.UTF_8))
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
