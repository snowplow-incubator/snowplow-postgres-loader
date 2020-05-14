package com.snowplowanalytics.snowplow.postgres.loader.streaming

import cats.Monad
import cats.data.{EitherT, NonEmptyList}
import cats.implicits._

import cats.effect.{ContextShift, Async, Bracket, Clock, Sync}
import cats.effect.concurrent.Ref

import fs2.Stream

import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor

import io.circe.Json

import com.snowplowanalytics.iglu.core.SchemaCriterion

import com.snowplowanalytics.iglu.client.{Resolver, Client}

import com.snowplowanalytics.iglu.schemaddl.StringUtils
import com.snowplowanalytics.iglu.schemaddl.jsonschema.{Pointer, Schema}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties.{Type => SType}
import com.snowplowanalytics.iglu.schemaddl.migrations.{FlatSchema, SchemaList => DdlSchemaList}

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{FailureDetails, Failure, BadRow, Payload}
import com.snowplowanalytics.snowplow.postgres.loader.Config.{ JdbcUri, processor }
import com.snowplowanalytics.snowplow.postgres.loader.shredding.{Type, Entity, transform}
import com.snowplowanalytics.snowplow.postgres.loader.shredding.schema.fetch

object sink {

  type Insert = ConnectionIO[Unit]

  def getTransactor[F[_]: Async: ContextShift](jdbcUri: JdbcUri, username: String, password: String): Transactor[F] =
    Transactor.fromDriverManager[F](
      "org.postgresql.Driver", jdbcUri.toString, username, password
    )

  def insertStatement(row: Entity): Insert = {
    val length = row.columns.length

    val columns = Fragment.const0(row.columns.map(_.name).mkString(","))

    val table = Fragment.const0(row.tableName)
    val values = row.columns.zipWithIndex.foldLeft(fr0"") {
      case (acc, (cur, i)) if i < length - 1 => acc ++ cur.value.fragment ++ fr0","
      case (acc, (cur, _)) => acc ++ cur.value.fragment
    }

    fr"""INSERT INTO $table ($columns) VALUES ($values)""".update(LogHandler.jdkLogHandler).run.void
  }

  type IgluErrors = NonEmptyList[FailureDetails.LoaderIgluError]

  object IgluErrors {
    def of(error: FailureDetails.LoaderIgluError): NonEmptyList[FailureDetails.LoaderIgluError] =
      NonEmptyList.of(error)
  }

  def badSink[F[_]: Sync](bads: Stream[F, BadRow]): Stream[F, Unit] =
    bads.evalMap(row => Sync[F].delay(println(row.compact)))

  def eventsSink[F[_]: Sync: Clock](xa: Transactor[F], state: Ref[F, PgState], client: Client[F, Json])
                                   (events: Stream[F, Event])
                                   (implicit B: Bracket[F, Throwable]): Stream[F, Unit] =
    events.evalMap { event =>
      val result = for {
        entities <- transform.shredEvent[F](client, event)
        insert <- insertData(client.resolver, state, entities).leftMap { errors =>
          BadRow.LoaderIgluError(processor, Failure.LoaderIgluErrors(errors), Payload.LoaderPayload(event)): BadRow
        }
      } yield insert

      result.value.flatMap {
        case Right(insert) => insert.transact(xa)
        case Left(badRow) =>
          Sync[F].delay(println(s"Bad row \n${badRow.selfDescribingData.data.spaces2}"))
      }
    }

  /**
   * Prepare tables for incoming data if necessary and insert the data
   * Tables will be updated/created if info is missing in `state`
   * @param resolver resolver to fetch missing schemas
   * @param state current state of the Postgres schema
   * @param event all shredded enitites from a single event, ready to be inserted
   */
  def insertData[F[_]: Sync: Clock](resolver: Resolver[F], state: Ref[F, PgState], event: List[Entity]): EitherT[F, IgluErrors, Insert] = {
    val inserts = event.parTraverse { entity =>
      val modelGroup = (entity.origin.vendor, entity.origin.name, entity.origin.version.model)
      val criterion = SchemaCriterion(entity.origin.vendor, entity.origin.name, "jsonschema", entity.origin.version.model)
      val tableMutation = EitherT.liftF[F, IgluErrors, PgState](state.get).flatMap { state =>
        state.check(modelGroup, entity.origin) match {
          case TableState.Missing =>
            EitherT(resolver.listSchemas(entity.origin.vendor, entity.origin.name, entity.origin.version.model))
              .leftMap(error => IgluErrors.of(FailureDetails.LoaderIgluError.SchemaListNotFound(criterion, error)))
              .flatMap(list => DdlSchemaList.fromSchemaList(list, fetch[F](resolver)).map(createTable[F]).leftMap(IgluErrors.of))
              .map(_.update(LogHandler.jdkLogHandler).run.void)
          case TableState.Match =>
            EitherT.rightT[F, IgluErrors](Monad[ConnectionIO].unit)
          case TableState.Outdated =>
            EitherT.rightT[F, IgluErrors](Monad[ConnectionIO].unit)
        }
      }

      tableMutation.map(mut => mut *> insertStatement(entity) )
    }

    inserts.map(_.sequence_)
  }


  def migrateTable[F[_]: Sync](schemaList: DdlSchemaList): F[Unit] = Sync[F].unit

  def createTable[F[_]](schemaList: DdlSchemaList): Fragment = {
    val subschemas = FlatSchema.extractProperties(schemaList)
    val tableName = StringUtils.getTableName(schemaList.latest)

    val columns = for {
      (pointer, schema) <- subschemas.filterNot { case (p, _) => p == Pointer.Root }
      columnName = FlatSchema.getName(pointer)
      dataType = Type.getDataType(schema, 4096, columnName, Type.dataTypeSuggestions)
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

  private[loader] sealed trait TableState extends Product with Serializable
  private[loader] object TableState {
    case object Match extends TableState
    case object Outdated extends TableState
    case object Missing extends TableState
  }
}
