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

import cats.implicits._

import io.circe.Json

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties.{Type => SType}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.NumberProperty.{Maximum, MultipleOf}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.StringProperty.{Format, MaxLength, MinLength}

sealed trait Type {
  def ddl: String =
    this match {
      case Type.Char(size)    => s"CHAR($size)"
      case Type.Varchar(size) => s"VARCHAR($size)"
      case Type.Uuid          => "UUID"
      case Type.Timestamp     => "TIMESTAMP"
      case Type.Date          => "DATE"
      case Type.Integer       => "INTEGER"
      case Type.BigInt        => "BIGINT"
      case Type.Double        => "DOUBLE PRECISION"
      case Type.Bool          => "BOOLEAN"
      case Type.Jsonb         => "JSONB"
    }
}

object Type {

  case class Char(size: Int) extends Type
  case class Varchar(size: Int) extends Type
  case object Uuid extends Type
  case object Timestamp extends Type
  case object Date extends Type
  case object Integer extends Type
  case object BigInt extends Type
  case object Double extends Type
  case object Bool extends Type
  case object Jsonb extends Type

  type DataTypeSuggestion = Schema => Option[Type]

  val DefaultVarcharSize = 4096

  /** Derive a Postgres type, given JSON Schema */
  def getDataType(properties: Schema, suggestions: List[DataTypeSuggestion]): Type =
    suggestions match {
      case Nil => Type.Jsonb // Generic
      case suggestion :: tail =>
        suggestion(properties) match {
          case Some(format) => format
          case None         => getDataType(properties, tail)
        }
    }

  // For complex enums Suggest VARCHAR with length of longest element
  val complexEnumSuggestion: DataTypeSuggestion = properties =>
    properties.enum match {
      case Some(enums) if isComplexEnum(enums.value) =>
        Some(Type.Jsonb)
      case _ => None
    }

  val productSuggestion: DataTypeSuggestion = properties =>
    properties.`type` match {
      case Some(t: SType.Union) if t.isUnion =>
        Some(Type.Jsonb)
      case Some(SType.Array) =>
        Some(Type.Jsonb)
      case Some(SType.Union(types)) if types.contains(SType.Array) =>
        Some(Type.Jsonb)
      case _ => None
    }

  val timestampSuggestion: DataTypeSuggestion = properties =>
    (properties.`type`, properties.format) match {
      case (Some(types), Some(Format.DateTimeFormat)) if types.possiblyWithNull(SType.String) =>
        Some(Type.Timestamp)
      case _ => None
    }

  val dateSuggestion: DataTypeSuggestion = properties =>
    (properties.`type`, properties.format) match {
      case (Some(types), Some(Format.DateFormat)) if types.possiblyWithNull(SType.String) =>
        Some(Type.Date)
      case _ => None
    }

  val numberSuggestion: DataTypeSuggestion = properties =>
    (properties.`type`, properties.multipleOf) match {
      case (Some(types), Some(MultipleOf.NumberMultipleOf(m))) if types.possiblyWithNull(SType.Number) && m === BigDecimal(1, 2) =>
        Some(Type.Double)
      case (Some(types), _) if types.possiblyWithNull(SType.Number) =>
        Some(Type.Double)
      case (Some(types: SType.Union), _) if (types.value - SType.Null) === Set(SType.Integer, SType.Number) =>
        Some(Type.Double)
      case _ =>
        None
    }

  // TODO: add more sizes
  val integerSuggestion: DataTypeSuggestion = properties => {
    (properties.`type`, properties.maximum, properties.enum, properties.multipleOf) match {
      case (Some(types), Some(maximum), _, _) if types.possiblyWithNull(SType.Integer) =>
        if (isBigInt(maximum)) Type.BigInt.some
        else Type.Integer.some
      case (Some(types), None, None, _) if types.possiblyWithNull(SType.Integer) =>
        Type.BigInt.some
      case (Some(types), _, Some(e), _) if types.possiblyWithNull(SType.Integer) =>
        if (isIntegerEnum(e.value)) Type.Integer.some
        else Type.BigInt.some
      case (None, _, Some(e), _) if isIntegerEnum(e.value) =>
          Type.Integer.some
      case (None, _, Some(e), _) if isBigIntEnum(e.value) =>
          Type.BigInt.some
      case (_, _, _, Some(MultipleOf.IntegerMultipleOf(_))) =>
        Type.Integer.some
      case _ => None
    }
  }

  val charSuggestion: DataTypeSuggestion = properties => {
    (properties.`type`, properties.minLength, properties.maxLength) match {
      case (Some(types), Some(MinLength(min)), Some(MaxLength(max))) if min === max && types.possiblyWithNull(SType.String) =>
        Some(Type.Char(min.toInt))
      case _ => None
    }
  }

  val booleanSuggestion: DataTypeSuggestion = properties => {
    (properties.`type`, properties.enum) match {
      case (Some(types), _) if types.possiblyWithNull(SType.Boolean) => Some(Type.Bool)
      case (None, Some(e)) if isBooleanEnum(e.value)                 => Some(Type.Bool)
      case _                                                         => None
    }
  }

  val uuidSuggestion: DataTypeSuggestion = properties => {
    (properties.`type`, properties.format) match {
      case (Some(types), Some(Format.UuidFormat)) if types.possiblyWithNull(SType.String) =>
        Some(Type.Uuid)
      case _ => None
    }
  }

  val varcharSuggestion: DataTypeSuggestion = properties => {
    (properties.`type`, properties.maxLength, properties.enum) match {
      case (Some(types), Some(maxLength), _) if types.possiblyWithNull(SType.String) =>
        Some(Type.Varchar(maxLength.value.toInt))
      case (Some(types), None, None) if types.possiblyWithNull(SType.String) =>
        Some(Type.Varchar(DefaultVarcharSize))
      case (_, _, Some(e)) if isStringEnum(e.value) =>
        e.value.flatMap(_.asString).map(_.length).maximumOption match {
          case Some(max) => Some(Type.Varchar(max))
          case None      => Some(Type.Varchar(DefaultVarcharSize))
        }
      case _ => None
    }
  }

  val dataTypeSuggestions: List[DataTypeSuggestion] = List(
    complexEnumSuggestion,
    productSuggestion,
    timestampSuggestion,
    dateSuggestion,
    integerSuggestion,
    numberSuggestion,
    booleanSuggestion,
    charSuggestion,
    uuidSuggestion,
    varcharSuggestion
  )

  /**
    * Get set of types or enum as string excluding null
    *
   * @param types comma-separated types
    * @return set of strings
    */
  private def excludeNull(types: List[Json]): List[Json] =
    types.filterNot(_.isNull)

  /**
    * Check enum contains some different types
    * (string and number or number and boolean)
    */
  private def isComplexEnum(enum: List[Json]) = {
    // Predicates
    def isNumeric(s: Json) = s.isNumber
    def isNonNumeric(s: Json) = !isNumeric(s)
    def isBoolean(s: Json) = s.isBoolean

    val nonNullEnum = excludeNull(enum)
    somePredicates(nonNullEnum, List(isNumeric _, isNonNumeric _, isBoolean _), 2)
  }

  def isBigInt(long: Maximum): Boolean =
    long match {
      case Maximum.IntegerMaximum(bigInt) => bigInt > Int.MaxValue
      case _                              => false
    }

  /**
    * Check at least some `quantity` of `predicates` are true on `instances`
    *
   * @param instances list of instances to check on
    * @param predicates list of predicates to check
    * @param quantity required quantity
    */
  private def somePredicates(instances: List[Json], predicates: List[Json => Boolean], quantity: Int): Boolean =
    if (quantity === 0) true
    else
      predicates match {
        case Nil                              => false
        case h :: tail if instances.exists(h) => somePredicates(instances, tail, quantity - 1)
        case _ :: tail                        => somePredicates(instances, tail, quantity)
      }

  private def isIntegerEnum(enum: List[Json]): Boolean =
    excludeNull(enum) match {
      case Nil => false
      case values => values.forall(_.asNumber.flatMap(_.toInt).isDefined)
    }

  private def isBigIntEnum(enum: List[Json]): Boolean =
    excludeNull(enum) match {
      case Nil => false
      case values => values.forall(_.asNumber.flatMap(_.toLong).isDefined)
    }

  private def isBooleanEnum(enum: List[Json]): Boolean =
    excludeNull(enum) match {
      case Nil => false
      case values => values.forall(_.isBoolean)
    }

  private def isStringEnum(enum: List[Json]): Boolean =
    excludeNull(enum) match {
      case Nil => false
      case values => values.forall(_.isString)
    }
}
