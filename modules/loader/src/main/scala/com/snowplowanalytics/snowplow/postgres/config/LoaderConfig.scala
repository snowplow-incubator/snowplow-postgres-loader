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
package com.snowplowanalytics.snowplow.postgres.config

import java.util.Date
import java.time.Instant
import java.nio.file.{Path => JPath}

import scala.jdk.CollectionConverters._
import scala.concurrent.duration._

import cats.syntax.either._

import fs2.aws.kinesis.KinesisCheckpointSettings

import io.circe.{Decoder}
import io.circe.generic.extras.semiauto.deriveConfiguredDecoder
import io.circe.generic.extras.Configuration
import io.circe.config.syntax._

import software.amazon.awssdk.regions.Region
import software.amazon.kinesis.common.{InitialPositionInStream, InitialPositionInStreamExtended}

import blobstore.Path

import com.snowplowanalytics.snowplow.postgres.config.LoaderConfig.Source.LocalFS.PathInfo
import LoaderConfig.{Purpose, Source}

case class LoaderConfig(input: Source,
                        output: DBConfig,
                        purpose: Purpose,
                        monitoring: LoaderConfig.Monitoring
)

object LoaderConfig {

  private[config] implicit def customCodecConfig: Configuration =
    Configuration.default.withDiscriminator("type")

  implicit val awsRegionDecoder: Decoder[Region] =
    Decoder.decodeString.emap { s =>
      val allRegions = Region.regions().asScala.toSet.map((r: Region) => r.id())
      if (allRegions.contains(s)) Region.of(s).asRight
      else s"Region $s is unknown, choose from [${allRegions.mkString(", ")}]".asLeft
    }

  sealed trait InitPosition {

    /** Turn it into aws-compatible structure */
    def unwrap: InitialPositionInStreamExtended =
      this match {
        case InitPosition.Latest            => InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)
        case InitPosition.TrimHorizon       => InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON)
        case InitPosition.AtTimestamp(timestamp) => InitialPositionInStreamExtended.newInitialPositionAtTimestamp(Date.from(timestamp))
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
          case Some("LATEST")       => Latest.asRight
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
              case None =>
                "Initial position can be either LATEST or TRIM_HORIZON string or AT_TIMESTAMP object (e.g. 2020-06-03T00:00:00Z)".asLeft
            }
        }
      }
  }

  sealed trait Purpose extends Product with Serializable
  object Purpose {
    case object Enriched extends Purpose
    case object SelfDescribing extends Purpose

    implicit def ioCirceConfigPurposeDecoder: Decoder[Purpose] =
      Decoder.decodeString.emap {
        case "ENRICHED_EVENTS" => Enriched.asRight
        case "JSON"            => SelfDescribing.asRight
        case other             => s"$other is not supported purpose, choose from ENRICHED_EVENTS and JSON".asLeft
      }
  }

  sealed trait Source extends Product with Serializable
  object Source {

    case class Kinesis(appName: String,
                       streamName: String,
                       region: Region,
                       initialPosition: InitPosition,
                       retrievalMode: Kinesis.Retrieval,
                       checkpointSettings: Kinesis.CheckpointSettings) extends Source
    object Kinesis {
      sealed trait Retrieval

      object Retrieval {
        case class Polling(maxRecords: Int) extends Retrieval
        case object FanOut extends Retrieval 

        implicit val retrievalDecoder: Decoder[Retrieval] = {
          Decoder.decodeString.emap {
            case "FanOut" => FanOut.asRight
            case "Polling" => "retrieval mode Polling must provide the maxRecords option".asLeft
            case other =>
              s"retrieval mode $other is unknown. Choose from FanOut and Polling. Polling must provide a MaxRecords option".asLeft
          }.or(deriveConfiguredDecoder[Retrieval])
        }
      }

      case class CheckpointSettings(maxBatchSize: Int, maxBatchWait: FiniteDuration) {
        def unwrap: KinesisCheckpointSettings =
          KinesisCheckpointSettings(maxBatchSize, maxBatchWait).toOption.get
      }

      object CheckpointSettings {
        implicit val kinesisCheckpointSettingsDecoder: Decoder[CheckpointSettings] =
          deriveConfiguredDecoder[CheckpointSettings].emap { settings =>
            KinesisCheckpointSettings(settings.maxBatchSize, settings.maxBatchWait)
              .leftMap(_.toString)
              .map(_ => settings)
          }
      }
    }

    case class LocalFS(path: PathInfo) extends Source
    object LocalFS {
      /**
        * Contains information about the given path
        * @param value Path which contains events
        * @param pathType Specifies whether given path is absolute or relative
        */
      case class PathInfo(value: Path, pathType: PathType) {
        /**
          * Combines root of the path and path itself.
          * Path root is determined according to the path type.
          */
        def allPath: JPath = JPath.of(pathType.fsroot.toString, "/", value.toString)
      }

      object PathInfo {
        implicit val pathDecoder: Decoder[PathInfo] =
          Decoder.decodeString.emap { pathStr =>
            Path.fromString(pathStr).toRight(s"Invalid path: $pathStr")
              .map(PathInfo(_, PathType.determineType(pathStr)))
          }
      }

      /**
        * Represents type of the path
        */
      sealed trait PathType {
        /**
          * Gives file system root according to the path type
          */
        def fsroot: JPath =
          this match {
            case PathType.Absolute => JPath.of("/")
            case PathType.Relative => JPath.of("./")
          }
      }

      object PathType {
        /**
          * If path starts from root directory, it's type is absolute
          */
        case object Absolute extends PathType

        /**
          * If path does not start from root directory, it's type is relative
          */
        case object Relative extends PathType

        /**
          * Determine type of the path according to where it starts
          */
        def determineType(path: String): PathType =
          if (path.startsWith("/")) LocalFS.PathType.Absolute
          else LocalFS.PathType.Relative
      }
    }

    case class PubSub(projectId: String, subscriptionId: String, checkpointSettings: PubSub.CheckpointSettings) extends Source

    object PubSub {
      case class CheckpointSettings(maxConcurrent: Int)

      object CheckpointSettings {
        implicit val pubsubCheckpointSettingsDecoder: Decoder[CheckpointSettings] =
          deriveConfiguredDecoder[CheckpointSettings]
      }
    }

    implicit def ioCirceConfigSourceDecoder: Decoder[Source] =
      deriveConfiguredDecoder[Source]
  }

  case class Monitoring(metrics: Monitoring.Metrics)

  object Monitoring {
    case class Metrics(cloudWatch: Boolean)

    implicit def metricsDecoder: Decoder[Metrics] =
      deriveConfiguredDecoder[Metrics]

    implicit def monitoringDecoder: Decoder[Monitoring] =
      deriveConfiguredDecoder[Monitoring]
  }

  implicit def ioCirceConfigDecoder: Decoder[LoaderConfig] =
    deriveConfiguredDecoder[LoaderConfig]

}
