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
package com.snowplowanalytics.snowplow.postgres

import doobie.implicits._

import com.snowplowanalytics.snowplow.postgres.storage.query

class queryspec extends Database {
  "listTables" should {
    "return single events table (after prepare executed)" >> {
      val expected = List("events")
      val result = query.listTables("public").transact(Database.xa).unsafeRunSync()

      result must beEqualTo(expected)
    }

    "return no tables (prepare executed only for 'public')" >> {
      val expected = List()
      val result = query.listTables("empty").transact(Database.xa).unsafeRunSync()

      result must beEqualTo(expected)
    }
  }

  "tableExists" should {
    "return false if table does not exist" >> {
      val expected = false
      val result = query.tableExists("empty", "non-existent", Database.logger).transact(Database.xa).unsafeRunSync()

      result must beEqualTo(expected)
    }

    "return true if table exists (created by Database.before)" >> {
      val expected = true
      val result = query.tableExists(Database.Schema, "events", Database.logger).transact(Database.xa).unsafeRunSync()

      result must beEqualTo(expected)
    }
  }

  "getComments" should {
    "not fail if schema does not exist" >> {
      val expected = List()
      val result = query.getComments("empty", Database.logger).transact(Database.xa).unsafeRunSync()
      result must beEqualTo(expected)
    }
  }
}
