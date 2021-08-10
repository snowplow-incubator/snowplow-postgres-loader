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

import cats.implicits._
import cats.effect.{ContextShift, Blocker, Resource, Timer, ConcurrentEffect, Sync}

import fs2.Pipe

import org.log4s.getLogger

import com.google.pubsub.v1.PubsubMessage

import com.permutive.pubsub.consumer.grpc.{PubsubGoogleConsumerConfig, PubsubGoogleConsumer}
import com.permutive.pubsub.consumer.Model.{ProjectId, Subscription}
import com.permutive.pubsub.consumer.decoder.MessageDecoder
import com.permutive.pubsub.consumer.ConsumerRecord

import com.snowplowanalytics.snowplow.postgres.config.LoaderConfig.Source
import com.snowplowanalytics.snowplow.postgres.streaming.SinkPipe
import com.snowplowanalytics.snowplow.postgres.streaming.data.BadData

object PubsubEnv {

  private lazy val logger = getLogger

  def create[F[_]: ConcurrentEffect: ContextShift: Timer](blocker: Blocker, config: Source.PubSub): Resource[F, Environment[F, ConsumerRecord[F, String]]] = {
    implicit val decoder: MessageDecoder[String] = pubsubDataDecoder
    val project = ProjectId(config.projectId)
    val subscription = Subscription(config.subscriptionId)
    val pubsubConfig = PubsubGoogleConsumerConfig[F](onFailedTerminate = pubsubOnFailedTerminate[F])
    Resource.eval(
      ConcurrentEffect[F].delay(
        Environment[F, ConsumerRecord[F, String]](
          PubsubGoogleConsumer.subscribe[F, String](blocker, project, subscription, pubsubErrorHandler[F], pubsubConfig),
          getPayload,
          checkpointer(config.checkpointSettings.maxConcurrent),
          SinkPipe.UnorderedPipe.forTransactor[F]
        )
      )
    )
  }

  private def getPayload[F[_]](record: ConsumerRecord[F, String]): Either[BadData, String] = record.value.asRight

  private def checkpointer[F[_]: ConcurrentEffect](maxConcurrent: Int): Pipe[F, ConsumerRecord[F, String], Unit] = _.parEvalMapUnordered(maxConcurrent)(_.ack)

  private def pubsubDataDecoder: MessageDecoder[String] =
    (message: Array[Byte]) => new String(message).asRight

  private def pubsubErrorHandler[F[_]: Sync](message: PubsubMessage, error: Throwable, ack: F[Unit], nack: F[Unit]): F[Unit] = {
    val _ = (error, nack)
    Sync[F].delay(logger.warn(s"Couldn't handle ${message.getData.toStringUtf8}")) *> ack
  }

  private def pubsubOnFailedTerminate[F[_]: Sync](error: Throwable): F[Unit] =
    Sync[F].delay(logger.warn(s"Cannot terminate pubsub consumer properly\n${error.getMessage}"))
}
