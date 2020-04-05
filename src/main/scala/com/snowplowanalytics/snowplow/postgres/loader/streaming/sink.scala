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
package com.snowplowanalytics.snowplow.postgres.loader.streaming

import cats.Monad
import cats.data.EitherT
import cats.implicits._

import cats.effect.{Sync, Clock, Concurrent}
import cats.effect.concurrent.Ref

import fs2.Pipe
import fs2.concurrent.Queue

import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor

import io.circe.Json

import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.iglu.client.{Resolver, Client}

import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, Payload}
import com.snowplowanalytics.snowplow.postgres.loader.config.Cli.processor
import com.snowplowanalytics.snowplow.postgres.loader.storage.{PgState, ddl}
import com.snowplowanalytics.snowplow.postgres.loader.shredding.{Entity, transform}
import com.snowplowanalytics.snowplow.postgres.loader.streaming.source.{Data, BadData}

object sink {

  type Insert = ConnectionIO[Unit]

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

  /** Sink bad data coming directly into the `Pipe` and data coming from `badQueue` */
  def badSink[F[_]: Concurrent](badQueue: Queue[F, BadData]): Pipe[F, BadData, Unit] =
    _.merge(badQueue.dequeue).evalMap {
      case BadData.BadEnriched(row) => Sync[F].delay(println(row.compact))
      case BadData.BadJson(payload, error) => Sync[F].delay(println(s"Cannot parse $payload. $error"))
    }

  /**
   * Sink good events into Postgres. During sinking, payloads go through all transformation steps
   * and checking the state of the DB itself.
   * Events that could not be transformed (due Iglu errors or DB unavailability) are sent back
   * to `badQueue`
   * @param xa DB transactor, responsible for connection pooling
   * @param logger impure SQL statement logger
   * @param schema database schema (e.g. `public` or `atomic`)
   * @param state mutable Loader state
   * @param badQueue queue where all unsucessful actions can unload its results
   * @param client Iglu Client
   */
  def goodSink[F[_]: Sync: Clock](xa: Transactor[F],
                                  logger: LogHandler,
                                  schema: String,
                                  state: Ref[F, PgState],
                                  badQueue: Queue[F, BadData],
                                  client: Client[F, Json]): Pipe[F, Data, Unit] =
    _.evalMap { payload =>
      val addMeta = payload.snowplow
      val result = for {
        entities <- payload match {
          case Data.Snowplow(event) => transform.shredEvent[F](client, event).leftMap(bad => BadData.BadEnriched(bad))
          case Data.SelfDescribing(json) => transform.shredJson(client)(json).map(entity => List(entity)).leftMap(errors => BadData.BadJson(json.normalize.noSpaces, errors.toString))
        }
        insert <- insertData(client.resolver, logger, schema, state, entities, addMeta).leftMap { errors =>
          payload match {
            case Data.Snowplow(event) =>
              val badRow = BadRow.LoaderIgluError(processor, Failure.LoaderIgluErrors(errors), Payload.LoaderPayload(event))
              BadData.BadEnriched(badRow)
            case Data.SelfDescribing(json) =>
              BadData.BadJson(json.normalize.noSpaces, s"Cannot insert: $errors")

          }
        }
      } yield insert

      result.value.flatMap {
        case Right(insert) => insert.transact[F](xa).attempt.flatMap {
          case Right(_) => Sync[F].unit
          case Left(e) => badQueue.enqueue1(BadData.BadJson(payload.toString, e.getMessage))
        }
        case Left(badRow) => badQueue.enqueue1(badRow)
      }
    }

  /**
   * Prepare tables for incoming data if necessary and insert the data
   * Tables will be updated/created if info is missing in `state`
   * @param resolver resolver to fetch missing schemas
   * @param state current state of the Postgres schema
   * @param event all shredded enitites from a single event, ready to be inserted
   */
  def insertData[F[_]: Sync: Clock](resolver: Resolver[F],
                                    logger: LogHandler,
                                    schema: String,
                                    state: Ref[F, PgState],
                                    event: List[Entity],
                                    meta: Boolean): EitherT[F, IgluErrors, Insert] = {
    val inserts = event.parTraverse { entity =>
      val tableMutation = EitherT.liftF[F, IgluErrors, PgState](state.get).flatMap { pgState =>
        pgState.check(entity.origin) match {
          case PgState.TableState.Match =>
            EitherT.rightT[F, IgluErrors](Monad[ConnectionIO].unit)
          case PgState.TableState.Missing =>
            ddl.createTable[F](resolver, state, logger, schema, entity, meta)
          case PgState.TableState.Outdated =>
            ddl.alterTable[F](resolver, state, logger, schema, entity)
        }
      }

      tableMutation.map(mut => mut *> insertStatement(logger, schema, entity))
    }

    inserts.map(_.sequence_)
  }
}
