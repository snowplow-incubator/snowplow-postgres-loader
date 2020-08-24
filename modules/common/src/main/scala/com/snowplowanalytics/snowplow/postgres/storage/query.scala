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

import cats.syntax.traverse._
import cats.syntax.either._
import cats.instances.list._

import doobie.ConnectionIO
import doobie.implicits._
import doobie.util.log.LogHandler

import com.snowplowanalytics.iglu.core.SchemaKey

/** Functions to query the storage for state and metadata */
object query {

  def tableExists(schema: String, name: String, logger: LogHandler): ConnectionIO[Boolean] =
    fr"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $name AND table_schema = $schema);"
      .queryWithLogHandler[Boolean](logger)
      .unique

  def listTables(schema: String): ConnectionIO[List[String]] =
    fr"SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = $schema".query[String].to[List]

  def getComment(schema: String, logger: LogHandler)(tableName: String): ConnectionIO[Either[CommentIssue, SchemaKey]] =
    (fr"""SELECT obj_description(oid) FROM pg_class WHERE relkind = 'r' AND relnamespace = (
            SELECT oid
            FROM pg_catalog.pg_namespace
            WHERE nspname = $schema
          ) AND relname = $tableName""")
      .queryWithLogHandler[Option[String]](logger) // It can be NULL, thus query[String].option will fail
      .unique
      .map {
        case Some(comment) =>
          SchemaKey.fromUri(comment) match {
            case Right(key)  => key.asRight
            case Left(error) => CommentIssue.Invalid(tableName, comment, error).asLeft
          }
        case None =>
          CommentIssue.Missing(tableName).asLeft
      }

  def getComments(schema: String, logger: LogHandler): ConnectionIO[List[Either[CommentIssue, SchemaKey]]] =
    listTables(schema).flatMap(_.traverse(getComment(schema, logger)))
}
