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
package com.snowplowanalytics.snowplow.postgres

import cats.Eq

import com.snowplowanalytics.iglu.core.SchemaKey

import com.snowplowanalytics.iglu.schemaddl.jsonschema.{JsonSchemaProperty, Pointer}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Pointer.{Cursor, SchemaProperty}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.properties.CommonProperties.Type

package object loader {
  implicit val typeEq: Eq[Type] = Eq.fromUniversalEquals[Type]
  implicit val schemaPropertyEq: Eq[SchemaProperty] = Eq.fromUniversalEquals[SchemaProperty]
  implicit val jsonSchemaPropertyEq: Eq[JsonSchemaProperty] = Eq.fromUniversalEquals[JsonSchemaProperty]
  implicit val cursorEq: Eq[Cursor] = Eq.fromUniversalEquals[Cursor]
  implicit val pointerEq: Eq[Pointer] = Eq.fromUniversalEquals[Pointer]
  implicit val schemaKeyEq: Eq[SchemaKey] = Eq.fromUniversalEquals[SchemaKey]
}
