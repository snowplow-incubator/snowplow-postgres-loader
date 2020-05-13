package com.snowplowanalytics.snowplow.postgres.loader.shredding

import com.snowplowanalytics.iglu.core.SchemaKey

import com.snowplowanalytics.snowplow.postgres.loader.shredding.Entity.Column

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
