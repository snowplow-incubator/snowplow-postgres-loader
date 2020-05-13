package com.snowplowanalytics.pgloader

import java.time.Instant

import doobie.{ Fragment, Meta }
import doobie.implicits._
import doobie.implicits.javasql._

sealed trait SqlValue {
  def column: String
  def fragment: Fragment
}
object SqlValue {
  case class Str(column: String, value: String) extends SqlValue {
    def fragment = fr0"$value"
  }
  case class Timestamp(column: String, value: java.sql.Timestamp) extends SqlValue {
    def fragment = fr0"$value"
  }
  object Timestamp {
    def apply(column: String, value: Instant): Timestamp =
      Timestamp(column, java.sql.Timestamp.from(value))
  }
  case class Integer(column: String, value: Int) extends SqlValue {
    def fragment = fr0"$value"
  }
  case class Double(column: String, value: scala.Double) extends SqlValue {
    def fragment = fr0"$value"
  }
  case class Bool(column: String, value: Boolean) extends SqlValue {
    def fragment = fr0"$value"
  }
}

