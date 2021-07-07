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
package com.snowplowanalytics.snowplow.postgres.shredding

import java.time.Instant
import java.time.format.DateTimeParseException
import java.util.UUID
import java.sql.Timestamp

import cats.data.{EitherNel, EitherT, NonEmptyList}
import cats.implicits._

import cats.effect.{Clock, Sync}

import io.circe.{ACursor, Json, JsonNumber}

import com.snowplowanalytics.iglu.core._

import com.snowplowanalytics.iglu.client.Client

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Pointer.SchemaPointer
import com.snowplowanalytics.iglu.schemaddl.{Properties, StringUtils}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.{Pointer, Schema}
import com.snowplowanalytics.iglu.schemaddl.migrations.FlatSchema

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, FailureDetails, Payload, Processor}
import Entity.Column
import Shredded.{ShreddedSelfDescribing, ShreddedSnowplow}

object transform {
  val Atomic = SchemaKey("com.snowplowanalytics.snowplow", "atomic", "jsonschema", SchemaVer.Full(1, 0, 0))

  /** Transform the whole `Event` (canonical and JSONs) into list of independent entities ready to be inserted */
  def shredEvent[F[_]: Sync: Clock](client: Client[F, Json], processor: Processor, event: Event): EitherT[F, BadRow, ShreddedSnowplow] = {
    val entities = event.contexts.data ++ event.derived_contexts.data ++ event.unstruct_event.data.toList
    val wholeEvent = entities.parTraverse(shredJson(client)).value.map { shreddedOrError =>
      (shreddedOrError, shredAtomic(Map())(event)).mapN { (shreddedEntities, atomic) =>
        ShreddedSnowplow(atomic, shreddedEntities.map(_.entity).map(addMetadata(event.event_id, event.collector_tstamp)))
      }
    }
    EitherT(wholeEvent).leftMap[BadRow](buildBadRow(processor, event))
  }

  def addMetadata(eventId: UUID, tstamp: Instant)(entity: Entity): Entity = {
    val metaColumns = List(
      Column("schema_vendor", Type.Varchar(128), Value.Varchar(entity.origin.vendor)),
      Column("schema_name", Type.Varchar(128), Value.Varchar(entity.origin.name)),
      Column("schema_format", Type.Varchar(16), Value.Varchar(entity.origin.format)),
      Column("schema_version", Type.Varchar(8), Value.Varchar(entity.origin.version.asString)),
      Column("root_id", Type.Uuid, Value.Uuid(eventId)),
      Column("root_tstamp", Type.Timestamp, Value.Timestamp(tstamp))
    )

    entity.copy(columns = metaColumns ++ entity.columns)
  }

  /** Remove all properties which are roots for other properties,
    *  Otherwise table will have structure of [nested, nested.a, nested.b],
    *  where we need just [nested.a, nested.b]
    */
  def removeRoots(props: Properties): Properties = {
    val pointers = props.map(_._1).toSet
    props.filterNot {
      case (pointer, _) =>
        pointer.value.isEmpty || {
          val problem = pointers.exists(p => pointer.isParentOf(p) && p != pointer)
          problem
        }
    }
  }

  /** Transform JSON into [[Entity]] */
  def shredJson[F[_]: Sync: Clock](
    client: Client[F, Json]
  )(data: SelfDescribingData[Json]): EitherT[F, NonEmptyList[FailureDetails.LoaderIgluError], ShreddedSelfDescribing] = {
    val key = data.schema
    schema.getOrdered(client.resolver)(key.vendor, key.name, key.version.model).leftMap(error => NonEmptyList.of(error)).subflatMap {
      properties =>
        val shredded = getNameTypeVal(properties)(data.data).parTraverse {
          case (columnName, pgType, value) =>
            cast(value, pgType).toEitherNel.map { value =>
              value.map(v => Entity.Column(columnName, pgType, v))
            }
        }

        shredded
          .leftMap { errors =>
            errors.map { error =>
              FailureDetails.LoaderIgluError.WrongType(data.schema, Json.Null, error) // TODO
            }
          }
          .map { cols =>
            val columns = cols.collect { case Some(c) => c }
            val tableName = data.schema match {
              case Atomic => "events"
              case other  => StringUtils.getTableName(SchemaMap(other))
            }
            ShreddedSelfDescribing(Entity(tableName, data.schema, columns))
          }
    }
  }

  /** Transform only canonical part of `Event` (128 non-JSON fields) into `ShreddedEntity` */
  def shredAtomic(lengths: Map[String, Int])(event: Event): EitherNel[FailureDetails.LoaderIgluError, Entity] = {
    def tranformDate(col: String)(s: String): Either[FailureDetails.LoaderIgluError, Entity.Column] =
      Either
        .catchOnly[DateTimeParseException](Instant.parse(s))
        .map(parsed => Entity.Column(col, Type.Timestamp, Value.Timestamp(parsed)))
        .leftMap(_ => FailureDetails.LoaderIgluError.WrongType(Atomic, Json.fromString(s), "date-time"))

    def transformUuid(col: String)(s: String): Either[FailureDetails.LoaderIgluError, Entity.Column] =
      Either
        .catchOnly[IllegalArgumentException](UUID.fromString(s))
        .map(parsed => Entity.Column(col, Type.Uuid, Value.Uuid(parsed)))
        .leftMap(_ => FailureDetails.LoaderIgluError.WrongType(Atomic, Json.fromString(s), "uuid"))

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
        case None      => Entity.Column(col, Type.Double, Value.Double(num.toDouble))
      }

    def castError(expected: String)(value: Json) =
      FailureDetails.LoaderIgluError.WrongType(Atomic, value, expected).asLeft[Option[Entity.Column]]

    val data = event.ordered.parTraverse {
      case ("contexts" | "derived_contexts" | "unstruct_event", _) =>
        none.asRight.toEitherNel
      case (key @ ("event_id" | "domain_sessionid"), Some(value)) =>
        val error = castError("uuid") _
        value.fold(
          none.asRight.toEitherNel,
          b => error(Json.fromBoolean(b)).toEitherNel,
          n => error(Json.fromJsonNumber(n)).toEitherNel,
          s => transformUuid(key)(s).map(_.some).toEitherNel,
          a => error(Json.arr(a: _*)).toEitherNel,
          o => error(Json.fromJsonObject(o)).toEitherNel
        )
      case (key, Some(value)) if key.endsWith("_tstamp") =>
        val error = castError("date-time") _
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
    data.map(_.unite).map(columns => Entity("events", Atomic, columns))
  }

  def cast(json: Option[Json], dataType: Type): Either[String, Option[Value]] = {
    val error = s"Invalid type ${dataType.ddl} for value $json".asLeft[Option[Value]]
    json.filterNot(_.isNull) match {
      case Some(j) =>
        dataType match {
          case Type.Uuid =>
            j.asString match {
              case Some(s) => Value.Uuid(UUID.fromString(s)).some.asRight // TODO
              case None    => error
            }
          case Type.Varchar(_) =>
            val result = j.asString match {
              case Some(s) => s
              case None    => j.noSpaces
            }
            Value.Varchar(result).some.asRight[String]
          case Type.Bool =>
            j.asBoolean match {
              case Some(b) => Value.Bool(b).some.asRight
              case None    => error
            }
          case Type.Char(len) =>
            j.asString match {
              case Some(s) if s.length === len => Value.Char(s).some.asRight
              case Some(_)                     => error
              case None                        => error
            }
          case Type.Integer =>
            j.asNumber.flatMap(_.toInt) match {
              case Some(int) => Value.Integer(int).some.asRight
              case None      => error
            }
          case Type.BigInt =>
            j.asNumber.flatMap(_.toLong) match {
              case Some(long) => Value.BigInt(long).some.asRight
              case None       => error
            }
          case Type.Double =>
            j.asNumber.map(_.toDouble) match {
              case Some(int) => Value.Double(int).some.asRight
              case None      => error
            }
          case Type.Jsonb =>
            Value.Jsonb(j).some.asRight
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

  /**
    * Transform Schema properties into information that can be transformed into DDL columns
    * It's very important to implement it and [[getNameTypeVal]] using same logic as
    * former is an implementation for DDL, while latter is implementation for data shredding
    * @return list of JSON Pointer, column name, inferred DB type, nullability
    */
  def getNameType(properties: Properties): List[(SchemaPointer, String, Type, Boolean)] =
    removeRoots(properties).map {
      case (pointer, s: Schema) =>
        val columnName: String = FlatSchema.getName(pointer)
        val pgType = Type.getDataType(s, Type.dataTypeSuggestions)
        (pointer, columnName, pgType, schema.canBeNull(s))
    }

  /**
    * Extract JSON Paths from an actual JSON data
    * It's very important to implement [[getNameType]] and this function using same logic as
    * former is an implementation for DDL, while latter is implementation for data shredding
    * @return list column name, inferred DB type, value
    */
  def getNameTypeVal(properties: Properties)(data: Json) =
    getNameType(properties).map {
      case (pointer, columnName, dataType, _) =>
        val value = getPath(pointer.forData, data)
        (columnName, dataType, value)
    }

  private def buildBadRow(processor: Processor, event: Event)(errors: NonEmptyList[FailureDetails.LoaderIgluError]) =
    BadRow.LoaderIgluError(processor, Failure.LoaderIgluErrors(errors), Payload.LoaderPayload(event))

}
