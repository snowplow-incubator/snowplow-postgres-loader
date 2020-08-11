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
package com.snowplowanalytics.snowplow.postgres.config

import cats.syntax.either._

import io.circe.Decoder
import LoaderConfig.{JdbcUri, Purpose}
import io.circe.generic.semiauto.deriveDecoder

case class LoaderConfig(host: String,
                        port: Int,
                        database: String,
                        username: String,
                        password: String, // TODO: can be EC2 store
                        sslMode: String,
                        schema: String,
                        purpose: Purpose) {
  def getJdbc: JdbcUri =
    JdbcUri(host, port, database, sslMode.toLowerCase().replace('_', '-'))
}

object LoaderConfig {

  sealed trait Purpose extends Product with Serializable {
    def snowplow: Boolean = this match {
      case Purpose.Enriched => true
      case Purpose.SelfDescribing => false
    }
  }
  object Purpose {
    case object Enriched extends Purpose
    case object SelfDescribing extends Purpose

    implicit def ioCirceConfigPurposeDecoder: Decoder[Purpose] =
      Decoder.decodeString.emap {
        case "ENRICHED_EVENTS" => Enriched.asRight
        case "JSON" => SelfDescribing.asRight
        case other => s"$other is not supported purpose, choose from ENRICHED_EVENTS and JSON".asLeft
      }
  }

  case class JdbcUri(host: String, port: Int, database: String, sslMode: String) {
    override def toString =
      s"jdbc:postgresql://$host:$port/$database?sslmode=$sslMode"
  }

  implicit def ioCirceConfigDecoder: Decoder[LoaderConfig] =
    deriveDecoder[LoaderConfig]

}
