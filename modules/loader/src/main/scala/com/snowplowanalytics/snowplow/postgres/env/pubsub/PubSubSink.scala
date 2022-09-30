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
package com.snowplowanalytics.snowplow.postgres.env.pubsub

import scala.jdk.CollectionConverters._

import cats.effect.{Async, Resource, Timer}
import cats.implicits._

import retry.syntax.all._

import com.permutive.pubsub.producer.Model.{ProjectId, Topic}
import com.permutive.pubsub.producer.encoder.MessageEncoder
import com.permutive.pubsub.producer.grpc.{GooglePubsubProducer, PubsubProducerConfig}
import com.permutive.pubsub.producer.PubsubProducer

import com.google.cloud.pubsub.v1.TopicAdminClient
import com.google.pubsub.v1.ProjectName

import org.log4s.getLogger

import com.snowplowanalytics.snowplow.postgres.streaming.StreamSink
import com.snowplowanalytics.snowplow.postgres.config.LoaderConfig
import com.snowplowanalytics.snowplow.postgres.config.LoaderConfig.BackoffPolicy

object PubSubSink {

  private lazy val logger = getLogger

  implicit val encoder: MessageEncoder[Array[Byte]] = new MessageEncoder[Array[Byte]] {
    override def encode(message: Array[Byte]): Either[Throwable, Array[Byte]] = message.asRight
  }

  def create[F[_]: Async: Timer](config: LoaderConfig.StreamSink.PubSub, backoffPolicy: BackoffPolicy): Resource[F, StreamSink[F]] = {
    val producerConfig = PubsubProducerConfig[F](
      // A batch of messages will be emitted to PubSub when the batch reaches the given size.
      batchSize = config.maxBatchSize,
      // A batch of messages will be emitted to PubSub when the batch reaches the given size.
      requestByteThreshold = Some(config.maxBatchBytes),
      // Set the delay threshold to use for batching. After this amount of time has elapsed (counting
      // from the first element added), the elements will be wrapped up in a batch and sent.
      delayThreshold = config.delayThreshold,
      onFailedTerminate = err => Async[F].delay(logger.error(err)("PubSub sink termination error"))
    )

    GooglePubsubProducer
      .of[F, Array[Byte]](ProjectId(config.projectId), Topic(config.topicId), producerConfig)
      .map(writeToPubSub(backoffPolicy))
  }

  private def writeToPubSub[F[_]: Async: Timer](backoffPolicy: BackoffPolicy)
                                               (producer: PubsubProducer[F, Array[Byte]])
                                               (data: Array[Byte]): F[Unit] =
    producer
      .produce(data)
      .retryingOnAllErrors(
        policy = backoffPolicy.retryPolicy,
        onError = (exception, retryDetails) => Async[F].delay(logger.warn(s"Error while writing record to PubSub - retries so far: ${retryDetails.retriesSoFar} - exception: $exception"))
      )
      .void

  def topicExists[F[_]: Async](config: LoaderConfig.StreamSink.PubSub): F[Boolean] =
    makeTopicAdminClient.use { client =>
      Async[F].delay(
        client.listTopics(ProjectName.of(config.projectId))
          .iterateAll()
          .asScala
          .toList
          .map(_.getName)
          .exists(_.contains(config.topicId))
      )
    }

  private def makeTopicAdminClient[F[_]: Async]: Resource[F, TopicAdminClient] =
    Resource.fromAutoCloseable {
      Async[F].delay(TopicAdminClient.create())
    }

}
