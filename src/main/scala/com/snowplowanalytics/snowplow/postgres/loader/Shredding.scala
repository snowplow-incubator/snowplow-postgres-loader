package com.snowplowanalytics.snowplow.postgres.loader

import java.time.Instant
import java.time.format.DateTimeParseException
import java.util.UUID
import java.sql.Timestamp

import cats.Monad
import cats.data.{EitherNel, EitherT, NonEmptyList}
import cats.implicits._
import cats.effect.{Clock, Sync}
import io.circe.{ACursor, Json, JsonNumber}
import doobie._
import doobie.implicits._
import doobie.implicits.javasql._
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaList, SchemaMap, SchemaVer, SelfDescribingData, SelfDescribingSchema}
import com.snowplowanalytics.iglu.client.{Client, ClientError, Resolver}
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties.{Type => SType}
import com.snowplowanalytics.iglu.schemaddl.{IgluSchema, ModelGroup, Properties, StringUtils}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Pointer.JsonPointer
import com.snowplowanalytics.iglu.schemaddl.jsonschema.{Pointer, Schema}
import com.snowplowanalytics.iglu.schemaddl.migrations.{FlatSchema, SchemaList => DdlSchemaList}
import com.snowplowanalytics.iglu.schemaddl.redshift.generators.DdlGenerator
import PgTypes.Type
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, FailureDetails, Payload}

object Shredding {


  case class ShreddedEntity(tableName: String, origin: SchemaKey, columns: List[PgTypes.Column])

  def shred[F[_]: Sync: Clock](client: Client[F, Json])(data: SelfDescribingData[Json]) = {
    val key = data.schema
    getOrdered(client.resolver)(key.vendor, key.name, key.version.model)
      .leftMap { error => NonEmptyList.of(error) }
      .subflatMap { properties =>
      val shredded = properties.parTraverse { case (pointer, schema) =>
        val value = getPath(pointer.forData, data.data)
        val columnName: String = FlatSchema.getName(pointer)
        val pgType = PgTypes.getDataType(schema, 4096, columnName, PgTypes.dataTypeSuggestions)
        cast(value, pgType).toEitherNel.map { value =>
          value.map { v => PgTypes.Column(columnName, pgType, v) }
        }
      }

      shredded
        .leftMap { errors => errors.map { error =>
          FailureDetails.LoaderIgluError.WrongType(data.schema, Json.Null, error)   // TODO
        } }
        .map { cols =>
          val columns = cols.collect { case Some(c) => c }
          val tableName = StringUtils.getTableName(SchemaMap(data.schema))
          ShreddedEntity(tableName, data.schema, columns)
        }
    }
  }

  val error = s"Invalid type".asLeft

  def cast(json: Option[Json], dataType: Type): Either[String, Option[PgTypes.Value]] = {
    json match {
      case Some(j) =>
        dataType match {
          case PgTypes.Type.Uuid =>
            j.asString match {
              case Some(s) => PgTypes.Value.Uuid(UUID.fromString(s)).some.asRight // TODO
              case None => error
            }
          case PgTypes.Type.Varchar(size) =>
            val result = j.asString match {
              case Some(s) => s
              case None => j.noSpaces
            }
            PgTypes.Value.Varchar(result).some.asRight[String]
          case PgTypes.Type.Bool =>
            j.asBoolean match {
              case Some(b) => PgTypes.Value.Bool(b).some.asRight
              case None => error
            }
          case PgTypes.Type.Char(len) =>
            j.asString match {
              case Some(s) if s.length == len => PgTypes.Value.Char(s).some.asRight
              case Some(_) => error
              case None => error
            }
          case PgTypes.Type.Integer =>
            j.asNumber.flatMap(_.toInt) match {
              case Some(int) => PgTypes.Value.Integer(int).some.asRight
              case None => error
            }
          case PgTypes.Type.Double =>
            j.asNumber.map(_.toDouble) match {
              case Some(int) => PgTypes.Value.Double(int).some.asRight
              case None => error
            }
          case PgTypes.Type.Date =>
            error // TODO
          case PgTypes.Type.Timestamp =>
            j.asString match {
              case Some(s) =>
                Either.catchOnly[DateTimeParseException](Instant.parse(s)).leftMap(_.getMessage).map { instant =>
                  PgTypes.Value.Timestamp(Timestamp.from(instant)).some
                }
              case None => error
            }
        }
    }
  }

  def getPath(pointer: JsonPointer, json: Json): Option[Json] = {
    def go(cursor: List[Pointer.Cursor], data: ACursor): Option[Json] =
      cursor match {
        case Nil =>
          data.focus
        case Pointer.Cursor.DownField(field) :: t =>
          go(t, data.downField(field))
        case Pointer.Cursor.At(i) :: t =>
          go(t, data.downN(i))
        case Pointer.Cursor.DownProperty(_) :: _ =>
          throw new IllegalStateException(s"Iglu Schema DDL tried to use invalid pointer ${pointer.show} for payload ${json.noSpaces}")
      }

    go(pointer.get, json.hcursor)
  }

  def getOrdered[F[_]: Monad: RegistryLookup: Clock](resolver: Resolver[F])
                                                    (vendor: String, name: String, model: Int): EitherT[F, FailureDetails.LoaderIgluError, Properties] = {
    def fetch(key: SchemaKey): EitherT[F, FailureDetails.LoaderIgluError, IgluSchema] =
      for {
        json <- EitherT(resolver.lookupSchema(key)).leftMap(error => FailureDetails.LoaderIgluError.IgluError(key, error): FailureDetails.LoaderIgluError)
        schema <- EitherT.fromEither[F](Schema.parse(json).toRight(buildFailure(json, key)))
      } yield SelfDescribingSchema(SchemaMap(key), schema)

    def buildFailure(json: Json, key: SchemaKey): FailureDetails.LoaderIgluError =
      FailureDetails.LoaderIgluError.InvalidSchema(key, s"JSON ${json.noSpaces} cannot be parsed as JSON Schema"): FailureDetails.LoaderIgluError

    val criterion = SchemaCriterion(vendor, name, "jsonschema", Some(model), None, None)
    val schemaList = resolver.listSchemas(vendor, name, model.some)
    for {
      schemaList <- EitherT[F, ClientError.ResolutionError, SchemaList](schemaList).leftMap(error => FailureDetails.LoaderIgluError.SchemaListNotFound(criterion, error))
      ordered <- DdlSchemaList.fromSchemaList(schemaList, fetch)
      properties = FlatSchema.extractProperties(ordered)
    } yield properties
  }

  def shredEntities[F[_]: Sync: Clock](client: Client[F, Json], event: Event): EitherT[F, BadRow, List[ShreddedEntity]] = {
    val entities = event.contexts.data ++ event.derived_contexts.data ++ event.unstruct_event.data.toList
    val wholeEvent = entities.parTraverse(shred(client)).value.map { either =>
      (either, shredAtomic(Map())(event)).mapN { (ents, atomic) => atomic :: ents }
    }
    EitherT(wholeEvent).leftMap(buildBadRow(event))
  }

  val Atomic = SchemaKey("com.snowplowanalytics", "snowplow", "jsonschema", SchemaVer.Full(1,0,0))

  def shredAtomic(lengths: Map[String, Int])(event: Event): EitherNel[FailureDetails.LoaderIgluError, ShreddedEntity] = {
    def tranformDate(col: String)(s: String): Either[FailureDetails.LoaderIgluError, PgTypes.Column] =
      Either
        .catchOnly[DateTimeParseException](Instant.parse(s))
        .map { parsed => PgTypes.Column(col, PgTypes.Type.Timestamp, PgTypes.Value.Timestamp(parsed)) }
        .leftMap { _ => FailureDetails.LoaderIgluError.WrongType(Atomic, Json.fromString(s), "date-time") }

    def transformBool(col: String)(b: Boolean): PgTypes.Column =
      if (b) PgTypes.Column(col, PgTypes.Type.Bool, PgTypes.Value.Bool(true))
      else PgTypes.Column(col, PgTypes.Type.Bool, PgTypes.Value.Bool(false))

    def truncate(col: String)(value: String): PgTypes.Column =
      lengths.get(col) match {
        case Some(len) =>
          PgTypes.Column(col, PgTypes.Type.Varchar(len), PgTypes.Value.Varchar(value.take(len)))
        case None =>
          PgTypes.Column(col, PgTypes.Type.Varchar(1024), PgTypes.Value.Varchar(value.take(1024)))
      }

    def transformNumber(col: String)(num: JsonNumber): PgTypes.Column =
      num.toInt match {
        case Some(int) => PgTypes.Column(col, PgTypes.Type.Integer, PgTypes.Value.Integer(int))
        case None => PgTypes.Column(col, PgTypes.Type.Double, PgTypes.Value.Double(num.toDouble))
      }

    def error(value: Json) =
      FailureDetails.LoaderIgluError.WrongType(Atomic, value, "date-time").asLeft[Option[PgTypes.Column]]

    val data = event.ordered.parTraverse {
      case ("contexts" | "derived_contexts" | "unstruct_event", _) =>
        none.asRight.toEitherNel
      case (key, Some(value)) if key.endsWith("_tstamp") =>
        value.fold(
          none.asRight.toEitherNel,
          b => error(Json.fromBoolean(b)).toEitherNel,
          n => error(Json.fromJsonNumber(n)).toEitherNel,
          s => tranformDate(key)(s).map(_.some).toEitherNel,
          a => error(Json.arr(a: _*)).toEitherNel,
          o => error(Json.fromJsonObject(o)).toEitherNel
        )
      case (key, Some(value)) =>
        value.fold(
          none.asRight.toEitherNel,
          b => transformBool(key)(b).some.asRight.toEitherNel,
          n => transformNumber(key)(n).some.asRight.toEitherNel,
          s => truncate(key)(s).some.asRight.toEitherNel,
          _ => none.asRight.toEitherNel,
          _ => none.asRight.toEitherNel
        )
      case (_, None) => none.asRight.toEitherNel
    }
    data.map(_.unite).map { columns => ShreddedEntity("events", Atomic, columns) }
  }

  private def buildBadRow(event: Event)(errors: NonEmptyList[FailureDetails.LoaderIgluError]) =
    BadRow.LoaderIgluError(Source.processor, Failure.LoaderIgluErrors(errors), Payload.LoaderPayload(event))

}
