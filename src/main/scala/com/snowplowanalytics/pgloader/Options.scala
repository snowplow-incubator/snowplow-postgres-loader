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

package com.snowplowanalytics.pgloader

import cats.data.ValidatedNel
import cats.implicits._
import com.monovore.decline._

object Options {
  case class JdbcUri(host: String, port: Int, database: String) {
    override def toString =
      s"jdbc:postgresql://$host:$port/$database"
  }

  object JdbcUri {
    def parseJdbcUri(string: String): Either[String, JdbcUri] = {
      val scheme = "jdbc:postgresql://"
      if (string.startsWith(scheme)) {
        val clean = string.drop(scheme.length)
        clean.split("/", -1).toList match {
          case host :: dbname :: Nil =>
            host.split(":").toList match {
              case name :: IntString(port) :: Nil =>
                JdbcUri(name, port, dbname).asRight
              case name :: Nil =>
                JdbcUri(name, 5432, dbname).asRight
              case _ :: invalidPort :: Nil =>
                s"JDBC port $invalidPort is not an integer".asLeft
            }
          case _ =>
            s"JDBC URI must contain host and database name separated by slash, got $string".asLeft
        }
      } else s"JDBC URI must start with $scheme, got $string".asLeft
    }

    implicit val jdbcUriArgument: Argument[JdbcUri] = new Argument[JdbcUri] {
      def read(string: String): ValidatedNel[String, JdbcUri] =
        parseJdbcUri(string).toValidatedNel

      def defaultMetavar: String = "URI"
    }


  }

  object IntString {
    def unapply(arg: String): Option[Int] =
      scala.util.Try(arg.toInt).toOption
  }

  val appName = Opts.option[String](
    long = "app-name",
    metavar = "name",
    help = "Input Kinesis app name"
  )

  val stream = Opts.option[String](
    long = "stream",
    metavar = "URI",
    help = "Input Kinesis stream name"
  )

  val jdbcUri = Opts.option[JdbcUri](
    long = "database",
    metavar = "URI",
    help = "Postgres sink"
  )

  val username = Opts.option[String](
    long = "username",
    metavar = "name",
    help = "Postgres username"
  )


  val password = Opts.option[String](
    long = "password",
    metavar = "secret",
    help = "Postgres password"
  )

  case class Config(appName: String, stream: String, jdbcUri: JdbcUri, username: String, password: String)

  val command = Command("Postgres Loader", "Snowplow Analytics Ltd.")((appName, stream, jdbcUri, username, password).mapN(Config.apply))
}
