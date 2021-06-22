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
package com.snowplowanalytics.snowplow.postgres.streaming

import java.util.Base64
import java.nio.charset.StandardCharsets

import cats.implicits._

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Resource, Sync, Timer}

import fs2.Stream
import fs2.aws.kinesis.{CommittableRecord, Kinesis, KinesisConsumerSettings}

import com.permutive.pubsub.consumer.grpc.{PubsubGoogleConsumer, PubsubGoogleConsumerConfig}
import io.circe.Json
import io.circe.parser.{parse => parseCirce}
import org.log4s.getLogger

import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.ParsingError.NotTSV
import com.snowplowanalytics.snowplow.badrows.{BadRow, Payload}
import com.snowplowanalytics.snowplow.postgres.config.{Cli, LoaderConfig}
import com.snowplowanalytics.snowplow.postgres.config.LoaderConfig.{Purpose, Source}
import com.snowplowanalytics.snowplow.postgres.streaming.data.{BadData, Data}

import com.google.pubsub.v1.PubsubMessage
import com.permutive.pubsub.consumer.Model.{ProjectId, Subscription}
import com.permutive.pubsub.consumer.decoder.MessageDecoder

import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient

object source {

  private lazy val logger = getLogger

  /**
    * Acquire a stream of parsed payloads
    *
   * @param blocker thread pool for pulling events (used only in PubSub)
    * @param purpose kind of data, enriched or plain JSONs
    * @param config source configuration
    * @return either error or stream of parsed payloads
    */
  def getSource[F[_]: ConcurrentEffect: ContextShift: Timer](blocker: Blocker, purpose: Purpose, config: Source): Stream[F, Either[BadData, Data]] =
    config match {
      case LoaderConfig.Source.Kinesis(appName, streamName, region, position) =>
        for {
          kinesisClient <- Stream.resource(makeKinesisClient[F])
          dynamoClient <- Stream.resource(makeDynamoDbClient[F])
          cloudWatchClient <- Stream.resource(makeCloudWatchClient[F])
          kinesis = Kinesis.create(kinesisClient, dynamoClient, cloudWatchClient, blocker)
          settings = KinesisConsumerSettings.apply(streamName, appName, region, initialPositionInStream = position.unwrap)
          record <- kinesis.readFromKinesisStream(settings)
          _ <- Stream.eval(record.checkpoint)
        } yield parseRecord(purpose, record)
      case LoaderConfig.Source.PubSub(projectId, subscriptionId) =>
        implicit val decoder: MessageDecoder[Either[BadData, Data]] = pubsubDataDecoder(purpose)
        val project = ProjectId(projectId)
        val subscription = Subscription(subscriptionId)
        val pubsubConfig = PubsubGoogleConsumerConfig[F](onFailedTerminate = pubsubOnFailedTerminate[F])
        PubsubGoogleConsumer
          .subscribeAndAck[F, Either[BadData, Data]](blocker, project, subscription, pubsubErrorHandler[F], pubsubConfig)
    }

  /**
    * Parse Kinesis record into a valid Loader's record, either enriched event or self-describing JSON,
    * depending on purpose of the Loader
    */
  def parseRecord(kind: Purpose, record: CommittableRecord): Either[BadData, Data] = {
    val string =
      try StandardCharsets.UTF_8.decode(record.record.data()).toString.asRight[BadData]
      catch {
        case _: IllegalArgumentException =>
          val payload = StandardCharsets.UTF_8.decode(Base64.getEncoder.encode(record.record.data())).toString
          kind match {
            case Purpose.Enriched =>
              val badRow = BadRow.LoaderParsingError(Cli.processor, NotTSV, Payload.RawPayload(payload))
              BadData.BadEnriched(badRow).asLeft
            case Purpose.SelfDescribing =>
              BadData.BadJson(payload, "Cannot deserialize self-describing JSON from Kinesis record").asLeft
          }
      }

    string.flatMap { payload =>
      kind match {
        case Purpose.Enriched =>
          parseEventString(payload).map(Data.Snowplow.apply)
        case Purpose.SelfDescribing =>
          parseJson(payload).map(Data.SelfDescribing.apply)
      }
    }
  }

  def parseEventString(s: String): Either[BadData, Event] =
    Event.parse(s).toEither.leftMap { error =>
      val badRow = BadRow.LoaderParsingError(Cli.processor, error, Payload.RawPayload(s))
      BadData.BadEnriched(badRow)
    }

  def parseJson(s: String): Either[BadData, SelfDescribingData[Json]] =
    parseCirce(s)
      .leftMap(_.show)
      .flatMap(json => SelfDescribingData.parse[Json](json).leftMap(_.message(json.noSpaces)))
      .leftMap(error => BadData.BadJson(s, error))

  def pubsubDataDecoder(purpose: Purpose): MessageDecoder[Either[BadData, Data]] =
    purpose match {
      case Purpose.Enriched =>
        (message: Array[Byte]) => parseEventString(new String(message)).map(Data.Snowplow.apply).asRight
      case Purpose.SelfDescribing =>
        (message: Array[Byte]) => parseJson(new String(message)).map(Data.SelfDescribing.apply).asRight
    }

  def pubsubErrorHandler[F[_]: Sync](message: PubsubMessage, error: Throwable, ack: F[Unit], nack: F[Unit]): F[Unit] = {
    val _ = (error, nack)
    Sync[F].delay(logger.warn(s"Couldn't handle ${message.getData.toStringUtf8}")) *> ack
  }

  def pubsubOnFailedTerminate[F[_]: Sync](error: Throwable): F[Unit] =
    Sync[F].delay(logger.warn(s"Cannot terminate pubsub consumer properly\n${error.getMessage}"))

  def makeKinesisClient[F[_]: Sync]: Resource[F, KinesisAsyncClient] =
    Resource.fromAutoCloseable(Sync[F].delay(KinesisAsyncClient.create()))

  def makeDynamoDbClient[F[_]: Sync]: Resource[F, DynamoDbAsyncClient] =
    Resource.fromAutoCloseable(Sync[F].delay(DynamoDbAsyncClient.create()))

  def makeCloudWatchClient[F[_]: Sync]: Resource[F, CloudWatchAsyncClient] =
    Resource.fromAutoCloseable(Sync[F].delay(CloudWatchAsyncClient.create()))
}
