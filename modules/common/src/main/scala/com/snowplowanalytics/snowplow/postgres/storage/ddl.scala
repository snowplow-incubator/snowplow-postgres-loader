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

import cats.data.EitherT
import cats.implicits._

import cats.effect.{Clock, Sync}

import doobie.ConnectionIO
import doobie.implicits._
import doobie.util.fragment.Fragment

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaMap}

import com.snowplowanalytics.iglu.client.Resolver

import com.snowplowanalytics.iglu.schemaddl.StringUtils
import com.snowplowanalytics.iglu.schemaddl.migrations.{SchemaList => DdlSchemaList}

import com.snowplowanalytics.snowplow.badrows.FailureDetails
import com.snowplowanalytics.snowplow.postgres.shredding.schema.fetch
import com.snowplowanalytics.snowplow.postgres.streaming.IgluErrors
import com.snowplowanalytics.snowplow.postgres.streaming.sink.Insert

object ddl {

  /** Function that can produce DDL, based on `DdlSchemaList` */
  type Generator = DdlSchemaList => Fragment

  def createTable[F[_]: Sync: Clock](resolver: Resolver[F],
                                     schema: String,
                                     entity: SchemaKey,
                                     meta: Boolean
  ): EitherT[F, IgluErrors, Insert] = {
    val generator: Generator = schemaList => sql.createTable(schema, entity, schemaList, meta)
    manage(resolver, schema, entity, generator)
  }

  // TODO: tables need to be updated in transaction, because situation where one node tries to mutate it after its state
  //       been update are completely predictable
  def alterTable[F[_]: Sync: Clock](resolver: Resolver[F], schema: String, entity: SchemaKey): EitherT[F, IgluErrors, Insert] = {
    val generator: Generator = schemaList => sql.migrateTable(schema, entity, schemaList)
    manage(resolver, schema, entity, generator)
  }

  def createEventsTable(schema: String): ConnectionIO[Unit] =
    definitions.atomicSql(schema).update().run.void

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
    * @param schema database schema
    * @param entity an actual shredded entity that we manage tables for
    * @param generator a function generating SQL from `DdlSchemaList`
    * @return an action that is either failure because of Iglu subsystem
    *         or doobie IO
    */
  def manage[F[_]: Sync: Clock](resolver: Resolver[F],
                                schema: String,
                                origin: SchemaKey,
                                generator: Generator
  ): EitherT[F, IgluErrors, Insert] = {
    val group = (origin.vendor, origin.name, origin.version.model)
    val criterion = SchemaCriterion(origin.vendor, origin.name, "jsonschema", origin.version.model)
    val (vendor, name, model) = group

    EitherT(resolver.listSchemas(vendor, name, model))
      .leftMap(error => IgluErrors.of(FailureDetails.LoaderIgluError.SchemaListNotFound(criterion, error)))
      .flatMap { list =>

        DdlSchemaList.fromSchemaList(list, fetch[F](resolver)).leftMap(IgluErrors.of).map { list =>
          val statement = generator(list)
          val tableName = StringUtils.getTableName(SchemaMap(origin))
          statement.update().run.void *>
            sql.commentTable(schema, tableName, list.latest)
        }
      }
  }
}
