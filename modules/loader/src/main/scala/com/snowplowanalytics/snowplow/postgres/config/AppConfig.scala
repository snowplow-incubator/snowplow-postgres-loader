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

import java.util.{UUID, Date}
import java.time.Instant

import scala.jdk.CollectionConverters._

import cats.syntax.either._

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredDecoder

import AppConfig.Source
import LoaderConfig.Purpose

import software.amazon.awssdk.regions.Region
import software.amazon.kinesis.common.InitialPositionInStream

case class AppConfig(name: String,
                     id: UUID,
                     source: Source,
                     host: String,
                     port: Int,
                     database: String,
                     username: String,
                     password: String, // TODO: can be EC2 store
                     sslMode: String,
                     schema: String,
                     purpose: Purpose) {
  def getLoaderConfig: LoaderConfig =
    LoaderConfig(host, port, database, username, password, sslMode, schema, purpose)
}

object AppConfig {

  implicit val awsRegionDecoder: Decoder[Region] =
    Decoder.decodeString.emap { s =>
      val allRegions = Region.regions().asScala.toSet.map((r: Region) => r.id())
      if (allRegions.contains(s)) Region.of(s).asRight
      else s"Region $s is unknown, choose from [${allRegions.mkString(", ")}]".asLeft
    }

  sealed trait InitPosition {
    /** Turn it into fs2-aws-compatible structure */
    def unwrap: Either[InitialPositionInStream, Date] = this match {
      case InitPosition.Latest => InitialPositionInStream.LATEST.asLeft
      case InitPosition.TrimHorizon => InitialPositionInStream.TRIM_HORIZON.asLeft
      case InitPosition.AtTimestamp(date) => Date.from(date).asRight
    }
  }
  object InitPosition {
    case object Latest extends InitPosition
    case object TrimHorizon extends InitPosition
    case class AtTimestamp(timestamp: Instant) extends InitPosition

    implicit val ioCirceInitPositionDecoder: Decoder[InitPosition] =
      Decoder.decodeJson.emap { json =>
        json.asString match {
          case Some("TRIM_HORIZON") => TrimHorizon.asRight
          case Some("LATEST") => Latest.asRight
          case Some(other) =>
            s"Initial position $other is unknown. Choose from LATEST and TRIM_HORIZEON. AT_TIMESTAMP must provide the timestamp".asLeft
          case None =>
            val result = for {
              root <- json.asObject.map(_.toMap)
              atTimestamp <- root.get("AT_TIMESTAMP")
              atTimestampObj <- atTimestamp.asObject.map(_.toMap)
              timestampStr <- atTimestampObj.get("timestamp")
              timestamp <- timestampStr.as[Instant].toOption
            } yield AtTimestamp(timestamp)
            result match {
              case Some(atTimestamp) => atTimestamp.asRight
              case None => "Initial position can be either LATEST or TRIM_HORIZON string or AT_TIMESTAMP object (e.g. 2020-06-03T00:00:00Z)".asLeft
            }
        }
      }
  }

  sealed trait Source extends Product with Serializable
  object Source {

    case class Kinesis(appName: String, streamName: String, region: Region, initialPosition: InitPosition) extends Source
    case class PubSub(projectId: String, subscriptionId: String) extends Source

    implicit val config: Configuration =
      Configuration.default.withSnakeCaseConstructorNames

    implicit def ioCirceConfigSourceDecoder: Decoder[Source] =
      deriveConfiguredDecoder[Source]
  }

  implicit def ioCirceConfigDecoder: Decoder[AppConfig] =
    deriveDecoder[AppConfig]

}
