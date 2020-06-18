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
package com.snowplowanalytics.snowplow.postgres.loader.storage

import cats.data.EitherT

import cats.effect.IO

import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingSchema, SchemaVer, SchemaMap, SchemaList => CoreSchemaList}

import com.snowplowanalytics.iglu.schemaddl.IgluSchema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.migrations.SchemaList

import com.snowplowanalytics.snowplow.postgres.loader.Database
import com.snowplowanalytics.snowplow.postgres.loader.storage.PgState.TableState

class PgStateSpec extends Database {
  "init" should {
    "initialize an empty state if no tables exist" >> {
      val state = PgState.init(Database.xa, Database.logger, Database.igluClient.resolver, "empty")
      val result = state
        .semiflatMap { case (issues, ref) => ref.get.map { state => (issues, state) } }
        .value
        .unsafeRunSync()
      val expected = (List(), PgState(Map()))
      result must beRight(expected)
    }
  }

  "check" should {
    "confirm table exists with a same key as in state" >> {
      val key = SchemaKey("com.acme", "event", "jsonschema", SchemaVer.Full(1,0,0))
      val schemaList = PgStateSpec.buildSchemaList(List(key))

      val init = Map(("com.acme", "event", 1) -> schemaList)
      val state = PgState(init)
      state.check(key) must beEqualTo(TableState.Match)
    }

    "claim table is outdated for 1-0-1 key if only 1-0-0 is known" >> {
      val key = SchemaKey("com.acme", "event", "jsonschema", SchemaVer.Full(1,0,0))
      val schemaList = PgStateSpec.buildSchemaList(List(key))

      val init = Map(("com.acme", "event", 1) -> schemaList)
      val state = PgState(init)
      state.check(SchemaKey("com.acme", "event", "jsonschema", SchemaVer.Full(1,0,1))) must beEqualTo(TableState.Outdated)
    }

    "claim table is missing for bumped model" >> {
      val key = SchemaKey("com.acme", "event", "jsonschema", SchemaVer.Full(1,0,0))
      val schemaList = PgStateSpec.buildSchemaList(List(key))

      val init = Map(("com.acme", "event", 1) -> schemaList)
      val state = PgState(init)
      state.check(SchemaKey("com.acme", "event", "jsonschema", SchemaVer.Full(2,0,0))) must beEqualTo(TableState.Missing)
    }

    "always assume events table exists" >> {
      val atomic = SchemaKey("com.snowplowanalytics.snowplow", "atomic", "jsonschema", SchemaVer.Full(1,0,0))
      val state = PgState(Map())
      state.check(atomic) must beEqualTo(TableState.Match)
    }
  }
}

object PgStateSpec {

  val fetch: SchemaKey => EitherT[IO, String, IgluSchema] =
    key => EitherT.pure[IO, String](SelfDescribingSchema(SchemaMap(key), Schema.empty))

  /** Bypass the `SchemaList` construction boilerplate */
  def buildSchemaList(keys: List[SchemaKey]): SchemaList = {
    val coreSchemaList = CoreSchemaList.parseUnsafe(keys)
    SchemaList.fromSchemaList(coreSchemaList, fetch).value.unsafeRunSync().getOrElse(throw new IllegalStateException)
  }
}
