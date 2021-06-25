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

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.{CommonProperties, NumberProperty, StringProperty}

import io.circe.Json

import org.specs2.mutable.Specification

class TypeSpec extends Specification {

  "getDataType" should {

    "return timestamp type for a datetime field" >> {
      val properties = Schema(`type` = Some(CommonProperties.Type.String), format = Some(StringProperty.Format.DateTimeFormat))
      Type.getDataType(properties, Type.dataTypeSuggestions) must_== Type.Timestamp
    }

    "return date type for a date field" >> {
      val properties = Schema(`type` = Some(CommonProperties.Type.String), format = Some(StringProperty.Format.DateTimeFormat))
      Type.getDataType(properties, Type.dataTypeSuggestions) must_== Type.Timestamp
    }

    "return jsonb type for an array field" >> {
      val properties = Schema(`type` = Some(CommonProperties.Type.Array))
      Type.getDataType(properties, Type.dataTypeSuggestions) must_== Type.Jsonb
    }

    "return bigint type for integer field with no maximum" >> {
      val properties = Schema(`type` = Some(CommonProperties.Type.Integer))
      Type.getDataType(properties, Type.dataTypeSuggestions) must_== Type.BigInt
    }

    "return integer type for integer field with small maximum" >> {
      val properties = Schema(`type` = Some(CommonProperties.Type.Integer), maximum = Some(NumberProperty.Maximum.IntegerMaximum(100)))
      Type.getDataType(properties, Type.dataTypeSuggestions) must_== Type.Integer
    }

    "return bigint type for integer field with large maximum" >> {
      val properties = Schema(`type` = Some(CommonProperties.Type.Integer), maximum = Some(NumberProperty.Maximum.IntegerMaximum(10000000000L)))
      Type.getDataType(properties, Type.dataTypeSuggestions) must_== Type.BigInt
    }

    "return double type for a number field" >> {
      val properties = Schema(`type` = Some(CommonProperties.Type.Number))
      Type.getDataType(properties, Type.dataTypeSuggestions) must_== Type.Double
    }

    "return boolean type for a boolean field" >> {
      val properties = Schema(`type` = Some(CommonProperties.Type.Boolean))
      Type.getDataType(properties, Type.dataTypeSuggestions) must_== Type.Bool
    }

    "return char type for a fixed length string field" >> {
      val properties = Schema(`type` = Some(CommonProperties.Type.String), minLength = Some(StringProperty.MinLength(42)), maxLength = Some(StringProperty.MaxLength(42)))
      Type.getDataType(properties, Type.dataTypeSuggestions) must_== Type.Char(42)
    }

    "return uuid type for a uuid field" >> {
      val properties = Schema(`type` = Some(CommonProperties.Type.String), format = Some(StringProperty.Format.UuidFormat))
      Type.getDataType(properties, Type.dataTypeSuggestions) must_== Type.Uuid
    }

    "return varchar type with max length for a string field" >> {
      val properties = Schema(`type` = Some(CommonProperties.Type.String), maxLength = (Some(StringProperty.MaxLength(42))))
      Type.getDataType(properties, Type.dataTypeSuggestions) must_== Type.Varchar(42)
    }

    "return varchar type for a string enum field" >> {
      val properties = Schema(enum = Some(CommonProperties.Enum(List(Json.fromString("xxx"), Json.fromString("yyy")))))
      Type.getDataType(properties, Type.dataTypeSuggestions) must_== Type.Varchar(3)
    }

    "return integer type for an integer enum field" >> {
      val properties = Schema(enum = Some(CommonProperties.Enum(List(Json.fromInt(1), Json.fromInt(2)))))
      Type.getDataType(properties, Type.dataTypeSuggestions) must_== Type.Integer
    }

    "return integer type for an integer enum field with type annotation" >> {
      val properties = Schema(enum = Some(CommonProperties.Enum(List(Json.fromInt(1), Json.fromInt(2)))), `type` = Some(CommonProperties.Type.Integer))
      Type.getDataType(properties, Type.dataTypeSuggestions) must_== Type.Integer
    }

    "return jsonb type for a complex enum field" >> {
      val properties = Schema(enum = Some(CommonProperties.Enum(List(Json.fromInt(12345), Json.fromString("y")))))
      Type.getDataType(properties, Type.dataTypeSuggestions) must_== Type.Jsonb
    }

  }
}
