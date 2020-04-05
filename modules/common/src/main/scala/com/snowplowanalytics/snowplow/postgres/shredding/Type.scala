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
package com.snowplowanalytics.snowplow.postgres.shredding

import cats.implicits._

import io.circe.Json

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties.{Type => SType}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.NumberProperty.{Maximum, MultipleOf}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.StringProperty.{Format, MaxLength, MinLength}

import com.snowplowanalytics.snowplow.postgres.loader._

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

  type DataTypeSuggestion = (Schema, String) => Option[Type]

  /** Derive a Postgres type, given JSON Schema */
  def getDataType(properties: Schema, varcharSize: Int, columnName: String, suggestions: List[DataTypeSuggestion]): Type =
    suggestions match {
      case Nil => Type.Varchar(4096) // Generic
      case suggestion :: tail =>
        suggestion(properties, columnName) match {
          case Some(format) => format
          case None         => getDataType(properties, varcharSize, columnName, tail)
        }
    }

  // For complex enums Suggest VARCHAR with length of longest element
  val complexEnumSuggestion: DataTypeSuggestion = (properties, _) =>
    properties.enum match {
      case Some(enums) if isComplexEnum(enums.value) =>
        val longest = excludeNull(enums.value).map(_.noSpaces.length).maximumOption.getOrElse(16)
        Some(Type.Varchar(longest))
      case _ => None
    }

  val productSuggestion: DataTypeSuggestion = (properties, _) =>
    properties.`type` match {
      case Some(t: SType.Union) if t.isUnion =>
        Some(Type.Jsonb)
      case Some(t: SType) if t === (SType.Array: SType) =>
        Some(Type.Jsonb)
      case Some(SType.Union(types)) if types.contains(SType.Array) =>
        Some(Type.Jsonb)
      case _ => None
    }

  val timestampSuggestion: DataTypeSuggestion = (properties, _) =>
    (properties.`type`, properties.format) match {
      case (Some(types), Some(Format.DateTimeFormat)) if types.possiblyWithNull(SType.String) =>
        Some(Type.Timestamp)
      case _ => None
    }

  val dateSuggestion: DataTypeSuggestion = (properties, _) =>
    (properties.`type`, properties.format) match {
      case (Some(types), Some(Format.DateFormat)) if types.possiblyWithNull(SType.String) =>
        Some(Type.Date)
      case _ => None
    }

  val arraySuggestion: DataTypeSuggestion = (properties, _) =>
    properties.`type` match {
      case Some(types) if types.possiblyWithNull(SType.Array) =>
        Some(Type.Varchar(4096))
      case _ => None
    }

  val numberSuggestion: DataTypeSuggestion = (properties, _) =>
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
  val integerSuggestion: DataTypeSuggestion = (properties, _) => {
    (properties.`type`, properties.maximum, properties.enum, properties.multipleOf) match {
      case (Some(types), Some(maximum), _, _) if types.possiblyWithNull(SType.Integer) =>
        if (isBigInt(maximum)) Type.BigInt.some
        else Type.Integer.some
      case (Some(types), None, _, _) if types.possiblyWithNull(SType.Integer) =>
        Type.BigInt.some
      // Contains only enum
      case (types, _, Some(_), _) if types.isEmpty || types.get.possiblyWithNull(SType.Integer) =>
        Type.Integer.some
      case (Some(types), _, _, _) if types.possiblyWithNull(SType.Integer) =>
        Type.Integer.some
      case (_, _, _, Some(MultipleOf.IntegerMultipleOf(_))) =>
        Type.Integer.some
      case _ => None
    }
  }

  val charSuggestion: DataTypeSuggestion = (properties, _) => {
    (properties.`type`, properties.minLength, properties.maxLength) match {
      case (Some(types), Some(MinLength(min)), Some(MaxLength(max))) if min === max && types.possiblyWithNull(SType.String) =>
        Some(Type.Char(min.toInt))
      case _ => None
    }
  }

  val booleanSuggestion: DataTypeSuggestion = (properties, _) => {
    properties.`type` match {
      case Some(types) if types.possiblyWithNull(SType.Boolean) => Some(Type.Bool)
      case _                                                    => None
    }
  }

  val uuidSuggestion: DataTypeSuggestion = (properties, _) => {
    (properties.`type`, properties.format) match {
      case (Some(types), Some(Format.UuidFormat)) if types.possiblyWithNull(SType.String) =>
        Some(Type.Uuid)
      case _ => None
    }
  }

  val varcharSuggestion: DataTypeSuggestion = (properties, _) => {
    (properties.`type`, properties.maxLength, properties.enum, properties.format) match {
      case (Some(types), Some(maxLength), _, _) if types.possiblyWithNull(SType.String) =>
        Some(Type.Varchar(maxLength.value.toInt))
      case (_, _, Some(enum), _) =>
        enum.value.map(jsonLength).maximumOption match {
          case Some(maxLength) if enum.value.lengthCompare(1) === 0 =>
            Some(Type.Varchar(maxLength))
          case Some(maxLength) =>
            Some(Type.Varchar(maxLength))
          case None => None
        }
      case _ => None
    }
  }

  val dataTypeSuggestions: List[DataTypeSuggestion] = List(
    complexEnumSuggestion,
    productSuggestion,
    timestampSuggestion,
    dateSuggestion,
    arraySuggestion,
    integerSuggestion,
    numberSuggestion,
    booleanSuggestion,
    charSuggestion,
    uuidSuggestion,
    varcharSuggestion
  )

  private def jsonLength(json: Json): Int =
    json.fold(0, b => b.toString.length, _ => json.noSpaces.length, _.length, _ => json.noSpaces.length, _ => json.noSpaces.length)

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
      case Maximum.IntegerMaximum(bigInt) => bigInt > 2147483647L
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
}
