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

import com.snowplowanalytics.iglu.core.SchemaKey

import Entity.Column

/** Final shredded entity */
case class Entity(tableName: String, origin: SchemaKey, columns: List[Column])

object Entity {

  /**
    * Table cell with value and meta info
    * @param name Postgres column name
    * @param dataType Postgres data type
    * @param value ready-to-be-inserted value
    */
  case class Column(name: String, dataType: Type, value: Value)

}
