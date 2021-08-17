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
package com.snowplowanalytics.snowplow.postgres.api

import cats.data.EitherT
import cats.implicits._

import cats.effect.{Bracket, Clock, Sync}

import doobie.implicits._
import doobie.util.transactor.Transactor

import com.snowplowanalytics.iglu.core.SchemaKey

import com.snowplowanalytics.iglu.client.Resolver

import com.snowplowanalytics.iglu.schemaddl.migrations.SchemaList

import com.snowplowanalytics.snowplow.postgres.shredding.{Entity, Shredded, schema}
import com.snowplowanalytics.snowplow.postgres.storage.ddl
import com.snowplowanalytics.snowplow.postgres.streaming.Sink

trait DB[F[_]] {
  def insert(event: List[Entity]): F[Unit]
  def alter(schemaKey: SchemaKey): F[Unit]
  def create(schemaKey: SchemaKey, includeMeta: Boolean): F[Unit]

  def getSchemaList(schemaKey: SchemaKey): F[SchemaList]
}

object DB {

  def apply[F[_]](implicit ev: DB[F]): DB[F] = ev

  def process[F[_]](shredded: Shredded, state: State[F])(implicit D: DB[F], B: Bracket[F, Throwable]): F[Unit] = {
    val (includeMeta, entities) = shredded match {
      case Shredded.ShreddedSnowplow(atomic, entities) => (true, atomic :: entities)
      case Shredded.ShreddedSelfDescribing(entity)     => (false, List(entity))
    }
    val insert = D.insert(entities)

    // Mutate table and Loader's mutable variable. Only for locked state!
    def mutate(missing: Set[SchemaKey], outdated: Set[SchemaKey]): F[Unit] =
      for {
        _ <- missing.toList.traverse(key => D.create(key, includeMeta)) // Create missing tables if any
        _ <- outdated.toList.traverse(D.alter) // Updated outdated tables if any
        _ <- (missing ++ outdated).toList.traverse_ { entity =>
          for { // Update state with new schemas
            list <- D.getSchemaList(entity)
            _ <- state.put(list)
          } yield ()
        }
      } yield ()

    state.checkAndRun(_.checkEvent(entities), insert, mutate)
  }

  sealed trait StateCheck {
    def missing: Set[SchemaKey]
    def outdated: Set[SchemaKey]

    final def add(entity: SchemaKey, state: TableState): StateCheck =
      state match {
        case TableState.Match    => this
        case TableState.Missing  => StateCheck.Block(missing + entity, outdated)
        case TableState.Outdated => StateCheck.Block(missing, outdated + entity)
      }
  }

  object StateCheck {
    case class Block(missing: Set[SchemaKey], outdated: Set[SchemaKey]) extends StateCheck
    case object Ok extends StateCheck {
      def missing: Set[SchemaKey] = Set.empty
      def outdated: Set[SchemaKey] = Set.empty
    }
  }

  def interpreter[F[_]: Sync: Clock](resolver: Resolver[F], xa: Transactor[F], schemaName: String): DB[F] =
    new DB[F] {
      def insert(event: List[Entity]): F[Unit] =
        event.traverse_(Sink.insertStatement(schemaName, _)).transact(xa)

      def alter(schemaKey: SchemaKey): F[Unit] = {
        val result = ddl.alterTable[F](resolver, schemaName, schemaKey)
        rethrow(result.semiflatMap(_.transact(xa)))
      }

      def create(schemaKey: SchemaKey, includeMeta: Boolean): F[Unit] = {
        val result = ddl.createTable[F](resolver, schemaName, schemaKey, includeMeta)
        rethrow(result.semiflatMap(_.transact(xa)))
      }

      def getSchemaList(schemaKey: SchemaKey): F[SchemaList] = {
        val result = schema.getSchemaList[F](resolver)(schemaKey.vendor, schemaKey.name, schemaKey.version.model)
        rethrow(result)
      }

      private def rethrow[A, E](f: EitherT[F, E, A]): F[A] =
        f.value.flatMap {
          case Right(result) => Sync[F].pure(result)
          case Left(error)   => Sync[F].raiseError(new RuntimeException(error.toString))
        }
    }
}
