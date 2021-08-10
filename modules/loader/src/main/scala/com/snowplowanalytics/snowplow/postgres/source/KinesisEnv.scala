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
package com.snowplowanalytics.snowplow.postgres.source

import java.util.{Base64, UUID}
import java.net.InetAddress
import java.nio.charset.StandardCharsets

import cats.implicits._

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Resource, Sync, Timer}

import fs2.{Stream, Pipe}
import fs2.aws.kinesis.{CommittableRecord, Kinesis}

import software.amazon.awssdk.regions.Region
import software.amazon.kinesis.common.ConfigsBuilder
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.metrics.MetricsLevel
import software.amazon.kinesis.processor.ShardRecordProcessorFactory
import software.amazon.kinesis.retrieval.polling.PollingConfig
import software.amazon.kinesis.retrieval.fanout.FanOutConfig
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient

import com.snowplowanalytics.snowplow.badrows.{BadRow, Payload}
import com.snowplowanalytics.snowplow.analytics.scalasdk.ParsingError.NotTSV

import com.snowplowanalytics.snowplow.postgres.config.Cli
import com.snowplowanalytics.snowplow.postgres.streaming.SinkPipe
import com.snowplowanalytics.snowplow.postgres.streaming.data.BadData
import com.snowplowanalytics.snowplow.postgres.config.LoaderConfig.{Monitoring, Source, Purpose}

object KinesisEnv {

  def create[F[_]: ConcurrentEffect: ContextShift: Timer](blocker: Blocker, config: Source.Kinesis, metrics: Monitoring.Metrics, purpose: Purpose): Resource[F, Environment[F, CommittableRecord]] =
    for {
      kinesisClient <- makeKinesisClient[F](config.region)
      dynamoClient <- makeDynamoDbClient[F](config.region)
      cloudWatchClient <- makeCloudWatchClient[F](config.region)
      kinesis = Kinesis.create(blocker, scheduler(kinesisClient, dynamoClient, cloudWatchClient, config, metrics, _))
    } yield Environment(
        getSource(kinesis),
        getPayload(purpose),
        checkpointer(kinesis, config.checkpointSettings),
        SinkPipe.OrderedPipe.forTransactor[F]
      )

  private def getSource[F[_]](kinesis: Kinesis[F]): Stream[F, CommittableRecord] =
    kinesis.readFromKinesisStream("THIS DOES NOTHING", "THIS DOES NOTHING")

  private def getPayload(purpose: Purpose)(record: CommittableRecord): Either[BadData, String] =
    Either.catchNonFatal(StandardCharsets.UTF_8.decode(record.record.data()).toString)
      .leftMap { _ =>
        val payload = StandardCharsets.UTF_8.decode(Base64.getEncoder.encode(record.record.data())).toString
        purpose match {
          case Purpose.Enriched =>
            val badRow = BadRow.LoaderParsingError(Cli.processor, NotTSV, Payload.RawPayload(payload))
            BadData.BadEnriched(badRow)
          case Purpose.SelfDescribing =>
            BadData.BadJson(payload, "\"Cannot deserialize self-describing JSON from record\"")
        }
      }

  private def checkpointer[F[_]](kinesis: Kinesis[F], checkpointSettings: Source.Kinesis.CheckpointSettings): Pipe[F, CommittableRecord, Unit] =
    kinesis.checkpointRecords(checkpointSettings.unwrap).andThen(_.as(()))

  private def scheduler[F[_]: Sync](kinesisClient: KinesisAsyncClient,
                            dynamoDbClient: DynamoDbAsyncClient,
                            cloudWatchClient: CloudWatchAsyncClient,
                            config: Source.Kinesis,
                            metrics: Monitoring.Metrics,
                            recordProcessorFactory: ShardRecordProcessorFactory): F[Scheduler] =
    Sync[F].delay(UUID.randomUUID()).map { uuid =>
      val hostname = InetAddress.getLocalHost().getCanonicalHostName()

      val configsBuilder =
        new ConfigsBuilder(config.streamName,
          config.appName,
          kinesisClient,
          dynamoDbClient,
          cloudWatchClient,
          s"$hostname:$uuid",
          recordProcessorFactory)

      val retrievalConfig =
        configsBuilder
          .retrievalConfig
          .initialPositionInStreamExtended(config.initialPosition.unwrap)
          .retrievalSpecificConfig {
            config.retrievalMode match {
              case Source.Kinesis.Retrieval.FanOut =>
                new FanOutConfig(kinesisClient).streamName(config.streamName).applicationName(config.appName)
              case Source.Kinesis.Retrieval.Polling(maxRecords) =>
                new PollingConfig(config.streamName, kinesisClient).maxRecords(maxRecords)
            }
          }

      val metricsConfig = configsBuilder.metricsConfig.metricsLevel {
        if (metrics.cloudWatch) MetricsLevel.DETAILED else MetricsLevel.NONE
      }

      new Scheduler(
        configsBuilder.checkpointConfig,
        configsBuilder.coordinatorConfig,
        configsBuilder.leaseManagementConfig,
        configsBuilder.lifecycleConfig,
        metricsConfig,
        configsBuilder.processorConfig,
        retrievalConfig
      )
    }

  private def makeKinesisClient[F[_]: Sync](region: Region): Resource[F, KinesisAsyncClient] =
    Resource.fromAutoCloseable {
      Sync[F].delay {
        KinesisAsyncClient.builder()
          .region(region)
          .build
      }
    }

  private def makeDynamoDbClient[F[_]: Sync](region: Region): Resource[F, DynamoDbAsyncClient] =
    Resource.fromAutoCloseable {
      Sync[F].delay {
        DynamoDbAsyncClient.builder()
          .region(region)
          .build
      }
    }

  private def makeCloudWatchClient[F[_]: Sync](region: Region): Resource[F, CloudWatchAsyncClient] =
    Resource.fromAutoCloseable {
      Sync[F].delay {
        CloudWatchAsyncClient.builder()
          .region(region)
          .build
      }
    }
}
