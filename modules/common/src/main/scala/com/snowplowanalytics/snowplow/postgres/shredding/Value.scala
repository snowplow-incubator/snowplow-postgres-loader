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

import java.sql.{Timestamp => JTimestamp}
import java.util.UUID
import java.time.Instant

import io.circe.Json

import doobie.syntax.string._
import doobie.implicits.javasql._
import doobie.postgres.implicits._
import doobie.postgres.circe.jsonb.implicits._
import doobie.util.fragment.Fragment

sealed trait Value {
  def fragment: Fragment =
    this match {
      case Value.Uuid(value)      => fr"$value"
      case Value.Char(value)      => fr"$value"
      case Value.Varchar(value)   => fr"$value"
      case Value.Timestamp(value) => fr"$value"
      case Value.Integer(value)   => fr"$value"
      case Value.BigInt(value)    => fr"$value"
      case Value.Double(value)    => fr"$value"
      case Value.Bool(value)      => fr"$value"
      case Value.Jsonb(value)     => fr"$value"
    }
}

object Value {
  case class Uuid(value: UUID) extends Value
  case class Char(value: String) extends Value
  case class Varchar(value: String) extends Value
  case class Timestamp(value: JTimestamp) extends Value
  case class Integer(value: Int) extends Value
  case class BigInt(value: Long) extends Value
  case class Double(value: scala.Double) extends Value
  case class Bool(value: Boolean) extends Value
  case class Jsonb(value: Json) extends Value

  object Timestamp {
    def apply(instant: Instant): Timestamp = Timestamp(JTimestamp.from(instant))
  }
}
