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
package com.snowplowanalytics.snowplow.postgres.env.kinesis

import java.util.{Base64, UUID}
import java.net.InetAddress
import java.nio.charset.StandardCharsets

import cats.Monad
import cats.implicits._
import cats.data.{EitherT, NonEmptyList}

import cats.effect.{Blocker, Clock, ConcurrentEffect, ContextShift, Resource, Sync, Timer}

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

import com.snowplowanalytics.snowplow.badrows.{BadRow, Payload, Failure}
import com.snowplowanalytics.snowplow.analytics.scalasdk.ParsingError.NotTSV

import com.snowplowanalytics.snowplow.postgres.config.Cli
import com.snowplowanalytics.snowplow.postgres.streaming.{SinkPipe, TimeUtils, StreamSink}
import com.snowplowanalytics.snowplow.postgres.config.LoaderConfig.{Source, Purpose, Monitoring}
import com.snowplowanalytics.snowplow.postgres.env.Environment
import com.snowplowanalytics.snowplow.postgres.env.kinesis.KinesisSink.mkKinesisClient

object KinesisEnv {

  def create[F[_]: ConcurrentEffect: ContextShift: Clock: Timer](blocker: Blocker,
                                                                 config: Source.Kinesis,
                                                                 badSink: StreamSink[F],
                                                                 metrics: Monitoring.Metrics,
                                                                 purpose: Purpose): Resource[F, Environment[F, CommittableRecord]] =
    for {
      kinesisClient <- mkKinesisClient[F](config.region)
      dynamoClient <- mkDynamoDbClient[F](config.region)
      cloudWatchClient <- mkCloudWatchClient[F](config.region)
      kinesis = Kinesis.create(blocker, scheduler(kinesisClient, dynamoClient, cloudWatchClient, config, metrics, _))
    } yield Environment(
        getSource(kinesis),
        badSink,
        getPayload[F](purpose, _),
        checkpointer(kinesis, config.checkpointSettings),
        SinkPipe.OrderedPipe.forTransactor[F]
      )

  private def getSource[F[_]](kinesis: Kinesis[F]): Stream[F, CommittableRecord] =
    // These arguments are used to create KinesisConsumerSettings later on.
    // However, only bufferSize field of created KinesisConsumerSettings object is used later on
    // therefore given stream name and app name are not used in anywhere.
    kinesis.readFromKinesisStream("THIS DOES NOTHING", "THIS DOES NOTHING")

  private def getPayload[F[_]: Clock: Monad](purpose: Purpose, record: CommittableRecord): F[Either[BadRow, String]] =
    EitherT.fromEither[F](Either.catchNonFatal(StandardCharsets.UTF_8.decode(record.record.data()).toString))
      .leftSemiflatMap[BadRow] { _ =>
        val payload = StandardCharsets.UTF_8.decode(Base64.getEncoder.encode(record.record.data())).toString
        purpose match {
          case Purpose.Enriched =>
            Monad[F].pure(BadRow.LoaderParsingError(Cli.processor, NotTSV, Payload.RawPayload(payload)))
          case Purpose.SelfDescribing =>
            TimeUtils.now[F].map { now =>
              val failure = Failure.GenericFailure(
                now,
                NonEmptyList.of("\"Cannot deserialize self-describing JSON from record\"")
              )
              BadRow.GenericError(Cli.processor, failure, Payload.RawPayload(payload))
            }
        }
      }.value

  private def checkpointer[F[_]](kinesis: Kinesis[F], checkpointSettings: Source.Kinesis.CheckpointSettings): Pipe[F, CommittableRecord, Unit] =
    kinesis.checkpointRecords(checkpointSettings.unwrap).andThen(_.void)

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

  private def mkDynamoDbClient[F[_]: Sync](region: Region): Resource[F, DynamoDbAsyncClient] =
    Resource.fromAutoCloseable {
      Sync[F].delay {
        DynamoDbAsyncClient.builder()
          .region(region)
          .build
      }
    }

  private def mkCloudWatchClient[F[_]: Sync](region: Region): Resource[F, CloudWatchAsyncClient] =
    Resource.fromAutoCloseable {
      Sync[F].delay {
        CloudWatchAsyncClient.builder()
          .region(region)
          .build
      }
    }
}
