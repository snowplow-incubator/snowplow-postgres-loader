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
package com.snowplowanalytics.snowplow.postgres.loader.storage

import cats.data.EitherT
import cats.implicits._

import cats.effect.{Sync, Clock}
import cats.effect.concurrent.Ref

import doobie.{ConnectionIO, LogHandler}
import doobie.implicits._
import doobie.util.fragment.Fragment

import com.snowplowanalytics.iglu.core.SchemaCriterion

import com.snowplowanalytics.iglu.client.Resolver

import com.snowplowanalytics.iglu.schemaddl.migrations.{SchemaList => DdlSchemaList}

import com.snowplowanalytics.snowplow.badrows.FailureDetails

import com.snowplowanalytics.snowplow.postgres.loader.shredding.Entity
import com.snowplowanalytics.snowplow.postgres.loader.shredding.schema.fetch
import com.snowplowanalytics.snowplow.postgres.loader.streaming.IgluErrors
import com.snowplowanalytics.snowplow.postgres.loader.streaming.sink.Insert


object ddl {

  /** Function that can produce DDL, based on `DdlSchemaList` */
  type Generator = DdlSchemaList => Fragment

  def createTable[F[_]: Sync: Clock](resolver: Resolver[F],
                                     state: Ref[F, PgState],
                                     logger: LogHandler,
                                     schema: String,
                                     entity: Entity,
                                     meta: Boolean): EitherT[F, IgluErrors, Insert] = {
    val generator: Generator = schemaList => sql.createTable(schema, entity, schemaList, meta)
    manage(resolver, state, logger, schema, entity, generator)
  }

  // TODO: tables need to be updated in transaction, because situation where one node tries to mutate it after its state
  //       been update are completely predictable
  def alterTable[F[_]: Sync: Clock](resolver: Resolver[F], state: Ref[F, PgState], logger: LogHandler, schema: String, entity: Entity): EitherT[F, IgluErrors, Insert] = {
    val generator: Generator = schemaList => sql.migrateTable(schema, entity, schemaList)
    manage(resolver, state, logger, schema, entity, generator)
  }

  def createEventsTable(schema: String, logger: LogHandler): ConnectionIO[Unit] =
    definitions.atomicSql(schema).update(logger).run.void

  /**
   * Perform some DB management: create or mutate the table according to current
   * schema state (where state is all known versions on the iglu registry)
   * First, check the current state of the schema on registry and validate it,
   * Then, create an actual update action using `generator` and comment on table
   * with latest schema from schema list retrieved from the registry
   * At last, update internal mutable state.
   *
   * Note that it doesn't actually perform a DB action (no `Transactor`)
   *
   * @param resolver Iglu Resolver tied to Iglu Server (it needs schema list endpoint)
   * @param state internal mutable state, containing all known schemas
   * @param logger doobie logger
   * @param schema database schema
   * @param entity an actual shredded entity that we manage tables for
   * @param generator a function generating SQL from `DdlSchemaList`
   * @return an action that is either failure because of Iglu subsystem
   *         or doobie IO
   */
  def manage[F[_]: Sync: Clock](resolver: Resolver[F],
                                state: Ref[F, PgState],
                                logger: LogHandler,
                                schema: String,
                                entity: Entity,
                                generator: Generator): EitherT[F, IgluErrors, Insert] = {
    val group = (entity.origin.vendor, entity.origin.name, entity.origin.version.model)
    val criterion = SchemaCriterion(entity.origin.vendor, entity.origin.name, "jsonschema", entity.origin.version.model)
    val (vendor, name, model) = group

    EitherT(resolver.listSchemas(vendor, name, model))
      .leftMap(error => IgluErrors.of(FailureDetails.LoaderIgluError.SchemaListNotFound(criterion, error)))
      .flatMap(list => DdlSchemaList.fromSchemaList(list, fetch[F](resolver)).map(l => l -> generator(l)).leftMap(IgluErrors.of))
      .map { case (list, statement) => (list, statement.update(logger).run.void) }
      .map { case (list, statement) => (list, statement *> sql.commentTable(logger, schema, entity.tableName, list.latest)) }
      .flatMap { case (list, insert) => EitherT.liftF[F, IgluErrors, Unit](state.update(_.put(list))).as(insert) }
  }
}
