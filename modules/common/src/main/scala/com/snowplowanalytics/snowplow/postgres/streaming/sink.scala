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

import cats.implicits._

import cats.effect.{Sync, Clock, Concurrent}

import fs2.Pipe
import fs2.concurrent.Queue

import doobie._
import doobie.implicits._

import io.circe.Json

import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.iglu.client.Client

import com.snowplowanalytics.snowplow.badrows.{BadRow, Payload, Processor}
import com.snowplowanalytics.snowplow.postgres.api.{State, DB}
import com.snowplowanalytics.snowplow.postgres.shredding.{Entity, transform}
import com.snowplowanalytics.snowplow.postgres.streaming.data.{Data, BadData}

object sink {

  type Insert = ConnectionIO[Unit]

  /**
   * Sink good events into Postgres. During sinking, payloads go through all transformation steps
   * and checking the state of the DB itself.
   * Events that could not be transformed (due Iglu errors or DB unavailability) are sent back
   * to `badQueue`
   * @param state mutable Loader state
   * @param badQueue queue where all unsucessful actions can unload its results
   * @param client Iglu Client
   * @param processor The actor processing these events
   */
  def goodSink[F[_]: Concurrent: Clock: DB](state: State[F],
                                            badQueue: Queue[F, BadData],
                                            client: Client[F, Json],
                                            processor: Processor): Pipe[F, Data, Unit] =
    _.parEvalMapUnordered(32)(sinkPayload(state, badQueue, client, processor))

  /** Sink bad data coming directly into the `Pipe` and data coming from `badQueue` */
  def badSink[F[_]: Concurrent](badQueue: Queue[F, BadData]): Pipe[F, BadData, Unit] =
    _.merge(badQueue.dequeue).evalMap {
      case BadData.BadEnriched(row) => Sync[F].delay(println(row.compact))
      case BadData.BadJson(payload, error) => Sync[F].delay(println(s"Cannot parse $payload. $error"))
    }

  /** Implementation for [[goodSink]] */
  def sinkPayload[F[_]: Sync: Clock: DB](state: State[F],
                                         badQueue: Queue[F, BadData],
                                         client: Client[F, Json],
                                         processor: Processor)(payload: Data): F[Unit] = {
    val result = for {
      entities <- payload match {
        case Data.Snowplow(event) =>
          transform
            .shredEvent[F](client, processor, event)
            .leftMap(bad => BadData.BadEnriched(bad))
        case Data.SelfDescribing(json) =>
          transform
            .shredJson(client)(json)
            .map(entity => List(entity))
            .leftMap(errors => BadData.BadJson(json.normalize.noSpaces, errors.toString))
      }
      insert = DB.process(entities, state).attempt.flatMap {
        case Right(_) => Sync[F].unit
        case Left(error) => payload match {
          case Data.Snowplow(event) =>
            val badRow = BadRow.LoaderRuntimeError(processor, error.getMessage, Payload.LoaderPayload(event))
            val pgBadRow = BadData.BadEnriched(badRow)
            badQueue.enqueue1(pgBadRow)
          case Data.SelfDescribing(json) =>
            val pgBadRow = BadData.BadJson(json.normalize.noSpaces, s"Cannot insert: ${error.getMessage}")
            badQueue.enqueue1(pgBadRow)

        }
      }
    } yield insert

    result.value.flatMap {
      case Right(action) => action
      case Left(error) => badQueue.enqueue1(error)
    }
  }

  /**
   * Build an `INSERT` action for a single entity
   * Multiple inserts later can be combined into a transaction
   */
  def insertStatement(logger: LogHandler, schema: String, row: Entity): Insert = {
    val length = row.columns.length

    val columns = Fragment.const0(row.columns.map(c => s"""\"${c.name}\"""").mkString(","))

    val table = Fragment.const0(s"$schema.${row.tableName}")
    val values = row.columns.zipWithIndex.foldLeft(fr0"") {
      case (acc, (cur, i)) if i < length - 1 => acc ++ cur.value.fragment ++ fr0","
      case (acc, (cur, _)) => acc ++ cur.value.fragment
    }

    fr"""INSERT INTO $table ($columns) VALUES ($values)""".update(logger).run.void
  }


}
