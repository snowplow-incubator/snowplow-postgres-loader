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

import java.nio.ByteBuffer
import java.util.UUID

import scala.concurrent.ExecutionContext
import scala.jdk.FutureConverters._
import scala.util.{Success, Failure}

import cats.effect.{Async, ContextShift, Resource, Sync, Timer}
import cats.implicits._

import fs2.aws.internal.{KinesisProducerClientImpl, KinesisProducerClient}

import retry.syntax.all._

import com.amazonaws.services.kinesis.producer.{UserRecordResult, KinesisProducerConfiguration}
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest
import software.amazon.awssdk.services.kinesis.model.StreamStatus
import software.amazon.awssdk.regions.Region

import com.google.common.util.concurrent.{Futures, ListenableFuture, FutureCallback, MoreExecutors}

import org.log4s.getLogger

import com.snowplowanalytics.snowplow.postgres.streaming.StreamSink
import com.snowplowanalytics.snowplow.postgres.config.LoaderConfig.{Monitoring, BackoffPolicy}
import com.snowplowanalytics.snowplow.postgres.config.LoaderConfig

object KinesisSink {

  lazy val logger = getLogger

  def create[F[_]: Async: Timer: ContextShift](config: LoaderConfig.StreamSink.Kinesis, monitoring: Monitoring, backoffPolicy: BackoffPolicy): Resource[F, StreamSink[F]] =
    mkProducer(config, monitoring).map(writeToKinesis(config.streamName, backoffPolicy))

  private def mkProducer[F[_]: Sync](config: LoaderConfig.StreamSink.Kinesis, monitoring: Monitoring): Resource[F, KinesisProducerClient[F]] =
    Resource.eval(
      Sync[F].delay(
        new KinesisProducerClientImpl[F](Some(mkProducerConfig(config, monitoring)))
      )
    )

  private def mkProducerConfig[F[_]](config: LoaderConfig.StreamSink.Kinesis, monitoring: Monitoring): KinesisProducerConfiguration = {
    val metricsLevel = if (monitoring.metrics.cloudWatch) "detailed" else "none"
    new KinesisProducerConfiguration()
      .setThreadingModel(KinesisProducerConfiguration.ThreadingModel.POOLED)
      .setRegion(config.region.toString)
      .setMetricsLevel(metricsLevel)
      .setCollectionMaxCount(config.maxBatchSize)
      .setCollectionMaxSize(config.maxBatchBytes)
      .setRecordMaxBufferedTime(config.delayThreshold.toMillis)
  }

  private def writeToKinesis[F[_]: Async: Timer: ContextShift](streamName: String,
                                                               backoffPolicy: BackoffPolicy)
                                                              (producer: KinesisProducerClient[F])
                                                              (data: Array[Byte]): F[Unit] = {
    val res = for {
      byteBuffer <- Async[F].delay(ByteBuffer.wrap(data))
      partitionKey <- Async[F].delay(UUID.randomUUID().toString)
      cb <- producer.putData(streamName, partitionKey, byteBuffer)
      cbRes <- registerCallback(cb)
      _ <- ContextShift[F].shift
    } yield cbRes
    res.retryingOnFailuresAndAllErrors(
      wasSuccessful = _.isSuccessful,
      policy = backoffPolicy.retryPolicy,
      onFailure = (_, retryDetails) => Async[F].delay(logger.warn(s"Failure while writing record to Kinesis - retries so far: ${retryDetails.retriesSoFar}")),
      onError = (exception, retryDetails) => Async[F].delay(logger.warn(s"Error while writing record to Kinesis - retries so far: ${retryDetails.retriesSoFar} - exception: $exception"))
    ).void
  }

  private def registerCallback[F[_]: Async](f: ListenableFuture[UserRecordResult]): F[UserRecordResult] =
    Async[F].async[UserRecordResult] { cb =>
      Futures.addCallback(
        f,
        new FutureCallback[UserRecordResult] {
          override def onFailure(t: Throwable): Unit = cb(Left(t))

          override def onSuccess(result: UserRecordResult): Unit = cb(Right(result))
        },
        MoreExecutors.directExecutor
      )
    }

  def streamExists[F[_]: Async: ContextShift](config: LoaderConfig.StreamSink.Kinesis)(implicit ec: ExecutionContext): F[Boolean] =
    mkKinesisClient(config.region).use { client =>
      Async[F].async[Boolean] { cb =>
        val describeStreamRequest = DescribeStreamRequest.builder()
          .streamName(config.streamName)
          .build()
        client.describeStream(describeStreamRequest)
          .asScala
          .onComplete {
            case Success(res) =>
              val streamStatus = res.streamDescription().streamStatus()
              cb(Right(streamStatus == StreamStatus.ACTIVE || streamStatus == StreamStatus.UPDATING))
            case Failure(_) => cb(Right(false))
          }
      }.flatMap { result =>
        ContextShift[F].shift.as(result)
      }
    }

  def mkKinesisClient[F[_]: Sync](region: Region): Resource[F, KinesisAsyncClient] =
    Resource.fromAutoCloseable {
      Sync[F].delay {
        KinesisAsyncClient.builder()
          .region(region)
          .build
      }
    }

}
