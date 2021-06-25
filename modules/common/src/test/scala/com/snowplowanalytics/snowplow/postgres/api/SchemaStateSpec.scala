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
package com.snowplowanalytics.snowplow.postgres.api

import cats.data.EitherT

import cats.effect.IO

import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingSchema, SchemaVer, SchemaMap, SchemaList => CoreSchemaList}

import com.snowplowanalytics.iglu.schemaddl.IgluSchema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.migrations.SchemaList

import com.snowplowanalytics.snowplow.postgres.Database

class SchemaStateSpec extends Database {
  "init" should {
    "initialize an empty state if no tables exist" >> {
      val state = SchemaState.init(List(), Database.igluClient.resolver)
      val result = state.semiflatMap(_.get).value.unsafeRunSync()
      val expected = SchemaState(Map())
      result must beRight(expected)
    }
  }

  "check" should {
    "confirm table exists with a same key as in state" >> {
      val key = SchemaKey("com.acme", "event", "jsonschema", SchemaVer.Full(1, 0, 0))
      val schemaList = SchemaStateSpec.buildSchemaList(List(key))

      val init = Map(("com.acme", "event", 1) -> schemaList)
      val state = SchemaState(init)
      state.check(key) must beEqualTo(TableState.Match)
    }

    "claim table is outdated for 1-0-1 key if only 1-0-0 is known" >> {
      val key = SchemaKey("com.acme", "event", "jsonschema", SchemaVer.Full(1, 0, 0))
      val schemaList = SchemaStateSpec.buildSchemaList(List(key))

      val init = Map(("com.acme", "event", 1) -> schemaList)
      val state = SchemaState(init)
      state.check(SchemaKey("com.acme", "event", "jsonschema", SchemaVer.Full(1, 0, 1))) must beEqualTo(TableState.Outdated)
    }

    "claim table is missing for bumped model" >> {
      val key = SchemaKey("com.acme", "event", "jsonschema", SchemaVer.Full(1, 0, 0))
      val schemaList = SchemaStateSpec.buildSchemaList(List(key))

      val init = Map(("com.acme", "event", 1) -> schemaList)
      val state = SchemaState(init)
      state.check(SchemaKey("com.acme", "event", "jsonschema", SchemaVer.Full(2, 0, 0))) must beEqualTo(TableState.Missing)
    }

    "always assume events table exists" >> {
      val atomic = SchemaKey("com.snowplowanalytics.snowplow", "atomic", "jsonschema", SchemaVer.Full(1, 0, 0))
      val state = SchemaState(Map())
      state.check(atomic) must beEqualTo(TableState.Match)
    }
  }
}

object SchemaStateSpec {

  val fetch: SchemaKey => EitherT[IO, String, IgluSchema] =
    key => EitherT.pure[IO, String](SelfDescribingSchema(SchemaMap(key), Schema.empty))

  /** Bypass the `SchemaList` construction boilerplate */
  def buildSchemaList(keys: List[SchemaKey]): SchemaList = {
    val coreSchemaList = CoreSchemaList.parseUnsafe(keys)
    SchemaList.fromSchemaList(coreSchemaList, fetch).value.unsafeRunSync().getOrElse(throw new IllegalStateException)
  }
}
