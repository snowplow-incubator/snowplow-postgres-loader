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

import cats.syntax.functor._

import doobie.Fragment
import doobie.free.connection.ConnectionIO
import doobie.implicits._
import org.log4s.getLogger

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaMap}

import com.snowplowanalytics.iglu.schemaddl.StringUtils
import com.snowplowanalytics.iglu.schemaddl.StringUtils.getTableName
import com.snowplowanalytics.iglu.schemaddl.jsonschema.{Pointer, Schema}
import com.snowplowanalytics.iglu.schemaddl.migrations.{FlatSchema, Migration, SchemaList}

import com.snowplowanalytics.snowplow.postgres.shredding.transform.Atomic
import com.snowplowanalytics.snowplow.postgres.shredding.{Type, schema, transform}
import com.snowplowanalytics.snowplow.postgres.logging.Slf4jLogHandler

object sql {

  private lazy val logger = Slf4jLogHandler(getLogger)

  /**
    * Generate the `CREATE TABLE` DDL statement
    * @param schema database schema
    * @param entity shredded entity
    * @param schemaList state of the
    * @param meta whether meta columns should be prepended
    * @return pure SQL expression with `CREATE TABLE` statement
    */
  def createTable(schema: String, entity: SchemaKey, schemaList: SchemaList, meta: Boolean): Fragment = {
    val subschemas = FlatSchema.extractProperties(schemaList)

    // Columns derived from schema (no metadata)
    val entityColumns = transform.getNameType(subschemas).map {
      case (_, columnName, dataType, nullability) =>
        definitions.columnToString(columnName, dataType, nullability)
    }

    val tableName = entity match {
      case Atomic => "events"
      case other  => StringUtils.getTableName(SchemaMap(other))
    }

    val columns = (if (meta) definitions.metaColumns.map((definitions.columnToString _).tupled) else Nil) ++ entityColumns
    val table = s"$schema.$tableName"

    Fragment.const(s"CREATE TABLE $table (\n${columns.mkString(",\n")}\n)")
  }

  def commentTable(schema: String, tableName: String, schemaKey: SchemaMap): ConnectionIO[Unit] = {
    val uri = schemaKey.schemaKey.toSchemaUri
    val table = s"$schema.$tableName"
    Fragment.const(s"COMMENT ON TABLE $table IS '$uri'").update(logger).run.void
  }

  def migrateTable(schema: String, entity: SchemaKey, schemaList: SchemaList) =
    schemaList match {
      case s: SchemaList.Full =>
        val migrationList = s.extractSegments.map(Migration.fromSegment)
        migrationList.find(_.from == entity.version) match {
          case Some(migration) =>
            val schemaMap = SchemaMap(migration.vendor, migration.name, "jsonschema", migration.to)
            val tableName = getTableName(schemaMap) // e.g. com_acme_event_1
            val tableNameFull = s"$schema.$tableName"

            if (migration.diff.added.nonEmpty) {
              val columns = migration.diff.added.map {
                case (pointer, schema) =>
                  buildColumn(pointer, schema)
              }

              val columnFragments = columns.foldLeft(Fragment.empty) { (acc, cur) =>
                val separator = if (acc == Fragment.empty) Fragment.const("\n") else Fragment.const(",\n")
                acc ++ separator ++ cur.toFragment
              }

              Fragment.const0(s"""ALTER TABLE $tableNameFull $columnFragments""")
            } else Fragment.empty
          case None =>
            Fragment.empty // TODO: This should be a warning
        }
      case _: SchemaList.Single =>
        Fragment.empty // TODO: This should be a warning
    }

  /**
    * Generate single ALTER TABLE statement for some new property
    *
    * @param pointer the property pointer
    * @param properties the property's schema, like length, maximum etc
    * @return DDL statement altering single column in table
    */
  def buildColumn(pointer: Pointer.SchemaPointer, properties: Schema): Column = {
    val columnName = FlatSchema.getName(pointer)
    val dataType = Type.getDataType(properties, Type.dataTypeSuggestions)
    Column(columnName, dataType, schema.canBeNull(properties))
  }

  case class Column(name: String, dataType: Type, nullable: Boolean) {

    /** "column_name VARCHAR(128) NOT NULL" */
    def toFragment: Fragment =
      Fragment.const0(s"$name ${dataType.ddl} ${if (nullable) "NULL" else "NOT NULL"}")
  }
}
