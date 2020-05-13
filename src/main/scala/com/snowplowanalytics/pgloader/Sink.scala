package com.snowplowanalytics.pgloader

import cats.syntax.functor._
import doobie._
import doobie.implicits._
import doobie.implicits.javasql._
import doobie.postgres.implicits._
import cats.effect.{Async, Clock, ContextShift, Sync}
import cats.effect.concurrent.Ref
import cats.implicits._
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.core.SchemaMap
import com.snowplowanalytics.iglu.schemaddl.{ModelGroup, StringUtils}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.{Pointer, Schema}
import com.snowplowanalytics.iglu.schemaddl.migrations.FlatSchema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties.{Type => SType}
import com.snowplowanalytics.iglu.schemaddl.migrations.{FlatSchema, SchemaList => DdlSchemaList}
import com.snowplowanalytics.pgloader.Options.JdbcUri
import com.snowplowanalytics.pgloader.Shredding.ShreddedEntity
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import io.circe.Json

object Sink {

  def getTransaction[F[_]: Async: ContextShift](jdbcUri: JdbcUri, username: String, password: String): Transactor[F] =
    Transactor.fromDriverManager[F](
      "org.postgresql.Driver", jdbcUri.toString, username, password
    )

  def insertAtomic(row: List[PgTypes.Column]): ConnectionIO[Unit] = {
    val length = row.length

    val columns = Fragment.const0(row.map(_.name).mkString(","))

    val values = row.zipWithIndex.foldLeft(fr0"") {
      case (acc, (cur, i)) if i < length - 1 => acc ++ fragment(cur.value) ++ fr0","
      case (acc, (cur, _)) => acc ++ fragment(cur.value)
    }

    fr"""INSERT INTO atomic.events ($columns) VALUES ($values)""".update(LogHandler.jdkLogHandler).run.void
  }

  def fragment(value: PgTypes.Value): Fragment = value match {
    case PgTypes.Value.Uuid(value) => fr"$value"
    case PgTypes.Value.Char(value) => fr"$value"
    case PgTypes.Value.Varchar(value) => fr"$value"
    case PgTypes.Value.Timestamp(value) => fr"$value"
    case PgTypes.Value.Integer(value) => fr"$value"
    case PgTypes.Value.Double(value) => fr"$value"
    case PgTypes.Value.Bool(value) => fr"$value"
  }


  case class PgState(tables: Map[ModelGroup, DdlSchemaList])

  def insertData[F[_]: Sync: Clock](resolver: Resolver[F], state: Ref[F, PgState], event: List[ShreddedEntity]) = {
    event.traverse { entity =>
      state.get.flatMap { s =>
        val modelGroup = (entity.origin.vendor, entity.origin.name, entity.origin.version.model)
        s.tables.get(modelGroup) match {
          case Some(DdlSchemaList.Full(schemas)) => schemas.toList.map(_.self.schemaKey).contains(entity.origin)
          case Some(DdlSchemaList.Single(schema)) => schema.self.schemaKey == entity.origin
          case None =>
            val q = EitherT(resolver.listSchemas(entity.origin.vendor, entity.origin.name, entity.origin.version.model.some))
            createTable(list)


        }
        s.tables.get(modelGroup).map(_.schemaKey == entity.origin) match {
          case None =>
            resolver.listSchemas(entity.origin.vendor, entity.origin.name, entity.origin.version.model.some).map {
              case Right(list) =>

              case Left(error) =>

            }
          case Some(true) => Sync[F].unit
          case Some(false) => migrateTable[F](???)
        }
      }

    }
  }


  def migrateTable[F[_]: Sync](schemaList: DdlSchemaList): F[Unit] = Sync[F].unit

  def createTable[F[_]](schemaList: DdlSchemaList): Fragment = {
    val subschemas = FlatSchema.extractProperties(schemaList)
    val tableName = StringUtils.getTableName(schemaList.latest)

    val columns = for {
      (pointer, schema) <- subschemas.filterNot { case (p, _) => p == Pointer.Root }
      columnName = FlatSchema.getName(pointer)
      dataType = PgTypes.getDataType(schema, 4096, columnName, PgTypes.dataTypeSuggestions)
      nullable = if (canBeNull(schema)) "NULL" else "NOT NULL"
    } yield s""""$columnName" ${dataType.ddl} $nullable"""

    Fragment.const(s"CREATE TABLE $tableName (\n${columns.mkString(",\n")}\n)")
  }

  private def canBeNull(schema: Schema): Boolean =
    schema.enum.exists(_.value.exists(_.isNull)) || (schema.`type` match {
      case Some(SType.Union(types)) => types.contains(SType.Null)
      case Some(t) => t == SType.Null
      case None => false
    })
}
