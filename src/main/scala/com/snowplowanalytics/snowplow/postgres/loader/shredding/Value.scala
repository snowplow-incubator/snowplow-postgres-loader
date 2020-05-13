package com.snowplowanalytics.snowplow.postgres.loader.shredding

import java.sql.{Timestamp => JTimestamp}
import java.util.UUID
import java.time.Instant

import doobie.syntax.string._
import doobie.implicits.javasql._
import doobie.postgres.implicits._
import doobie.util.fragment.Fragment

sealed trait Value {
  def fragment: Fragment = this match {
    case Value.Uuid(value) => fr"$value"
    case Value.Char(value) => fr"$value"
    case Value.Varchar(value) => fr"$value"
    case Value.Timestamp(value) => fr"$value"
    case Value.Integer(value) => fr"$value"
    case Value.Double(value) => fr"$value"
    case Value.Bool(value) => fr"$value"
  }
}

object Value {
  case class Uuid(value: UUID) extends Value
  case class Char(value: String) extends Value
  case class Varchar(value: String) extends Value
  case class Timestamp(value: JTimestamp) extends Value
  case class Integer(value: Int) extends Value
  case class Double(value: scala.Double) extends Value
  case class Bool(value: Boolean) extends Value

  object Timestamp {
    def apply(instant: Instant): Timestamp = Timestamp(JTimestamp.from(instant))
  }
}

