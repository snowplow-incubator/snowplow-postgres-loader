package com.snowplowanalytics.snowplow.postgres.loader.shredding

import java.time.Instant
import java.time.format.DateTimeParseException
import java.util.UUID
import java.sql.Timestamp

import cats.data.{EitherT, EitherNel, NonEmptyList}
import cats.implicits._

import cats.effect.{Sync, Clock}

import io.circe.{JsonNumber, Json, ACursor}

import com.snowplowanalytics.iglu.core._

import com.snowplowanalytics.iglu.client.Client

import com.snowplowanalytics.iglu.schemaddl.StringUtils
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Pointer
import com.snowplowanalytics.iglu.schemaddl.migrations.FlatSchema

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{FailureDetails, BadRow, Failure, Payload}

import com.snowplowanalytics.snowplow.postgres.loader.Config

object transform {
  val Atomic = SchemaKey("com.snowplowanalytics.snowplow", "atomic", "jsonschema", SchemaVer.Full(1,0,0))

  /** Transform the whole `Event` (canonical and JSONs) into list of independent entities ready to be inserted */
  def shredEvent[F[_]: Sync: Clock](client: Client[F, Json], event: Event): EitherT[F, BadRow, List[Entity]] = {
    val entities = event.contexts.data ++ event.derived_contexts.data ++ event.unstruct_event.data.toList
    val wholeEvent = entities.parTraverse(shredJson(client)).value.map { either =>
      (either, shredAtomic(Map())(event)).mapN { (ents, atomic) => atomic :: ents }
    }
    EitherT(wholeEvent).leftMap[BadRow](buildBadRow(event))
  }

  /** Transform JSON into [[Entity]] */
  def shredJson[F[_]: Sync: Clock](client: Client[F, Json])
                                  (data: SelfDescribingData[Json]): EitherT[F, NonEmptyList[FailureDetails.LoaderIgluError], Entity] = {
    val key = data.schema
    schema.getOrdered(client.resolver)(key.vendor, key.name, key.version.model)
      .leftMap { error => NonEmptyList.of(error) }
      .subflatMap { properties =>
      val shredded = properties.parTraverse { case (pointer, schema) =>
        val value = getPath(pointer.forData, data.data)
        val columnName: String = FlatSchema.getName(pointer)
        val pgType = Type.getDataType(schema, 4096, columnName, Type.dataTypeSuggestions)
        cast(value, pgType).toEitherNel.map { value =>
          value.map { v => Entity.Column(columnName, pgType, v) }
        }
      }

      shredded
        .leftMap { errors => errors.map { error =>
          FailureDetails.LoaderIgluError.WrongType(data.schema, Json.Null, error)   // TODO
        } }
        .map { cols =>
          val columns = cols.collect { case Some(c) => c }
          val tableName = data.schema match {
            case Atomic => "events"
            case other => StringUtils.getTableName(SchemaMap(other))
          }
          Entity(tableName, data.schema, columns)
        }
    }
  }

  /** Transform only canonical part of `Event` (128 non-JSON fields) into `ShreddedEntity` */
  def shredAtomic(lengths: Map[String, Int])(event: Event): EitherNel[FailureDetails.LoaderIgluError, Entity] = {
    def tranformDate(col: String)(s: String): Either[FailureDetails.LoaderIgluError, Entity.Column] =
      Either
        .catchOnly[DateTimeParseException](Instant.parse(s))
        .map { parsed => Entity.Column(col, Type.Timestamp, Value.Timestamp(parsed)) }
        .leftMap { _ => FailureDetails.LoaderIgluError.WrongType(Atomic, Json.fromString(s), "date-time") }

    def transformBool(col: String)(b: Boolean): Entity.Column =
      if (b) Entity.Column(col, Type.Bool, Value.Bool(true))
      else Entity.Column(col, Type.Bool, Value.Bool(false))

    def truncate(col: String)(value: String): Entity.Column =
      lengths.get(col) match {
        case Some(len) =>
          Entity.Column(col, Type.Varchar(len), Value.Varchar(value.take(len)))
        case None =>
          Entity.Column(col, Type.Varchar(1024), Value.Varchar(value.take(1024)))
      }

    def transformNumber(col: String)(num: JsonNumber): Entity.Column =
      num.toInt match {
        case Some(int) => Entity.Column(col, Type.Integer, Value.Integer(int))
        case None => Entity.Column(col, Type.Double, Value.Double(num.toDouble))
      }

    def error(value: Json) =
      FailureDetails.LoaderIgluError.WrongType(Atomic, value, "date-time").asLeft[Option[Entity.Column]]

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
    data.map(_.unite).map { columns => Entity("events", Atomic, columns) }
  }

  def cast(json: Option[Json], dataType: Type): Either[String, Option[Value]] = {
    val error = s"Invalid type ${dataType.ddl} for value $json".asLeft[Option[Value]]
    json match {
      case Some(j) =>
        dataType match {
          case Type.Uuid =>
            j.asString match {
              case Some(s) => Value.Uuid(UUID.fromString(s)).some.asRight // TODO
              case None => error
            }
          case Type.Varchar(size) =>
            val result = j.asString match {
              case Some(s) => s
              case None => j.noSpaces
            }
            Value.Varchar(result).some.asRight[String]
          case Type.Bool =>
            j.asBoolean match {
              case Some(b) => Value.Bool(b).some.asRight
              case None => error
            }
          case Type.Char(len) =>
            j.asString match {
              case Some(s) if s.length == len => Value.Char(s).some.asRight
              case Some(_) => error
              case None => error
            }
          case Type.Integer =>
            j.asNumber.flatMap(_.toInt) match {
              case Some(int) => Value.Integer(int).some.asRight
              case None => error
            }
          case Type.BigInt =>
            j.asNumber.flatMap(_.toLong) match {
              case Some(long) => Value.BigInt(long).some.asRight
              case None => error
            }
          case Type.Double =>
            j.asNumber.map(_.toDouble) match {
              case Some(int) => Value.Double(int).some.asRight
              case None => error
            }
          case Type.Date =>
            error // TODO
          case Type.Timestamp =>
            j.asString match {
              case Some(s) =>
                Either.catchOnly[DateTimeParseException](Instant.parse(s)).leftMap(_.getMessage).map { instant =>
                  Value.Timestamp(Timestamp.from(instant)).some
                }
              case None => error
            }
        }
      case None => none.asRight
    }
  }

  def getPath(pointer: Pointer.JsonPointer, json: Json): Option[Json] = {
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

  private def buildBadRow(event: Event)(errors: NonEmptyList[FailureDetails.LoaderIgluError]) =
    BadRow.LoaderIgluError(Config.processor, Failure.LoaderIgluErrors(errors), Payload.LoaderPayload(event))

}
