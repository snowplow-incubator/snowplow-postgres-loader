package com.snowplowanalytics.pgloader

import java.nio.charset.StandardCharsets

import cats.data.Validated
import cats.implicits._
import cats.effect.{ConcurrentEffect, ContextShift}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import fs2.aws.kinesis.{CommittableRecord, KinesisConsumerSettings}
import fs2.aws.kinesis.consumer.readFromKinesisStream
import software.amazon.awssdk.regions.Region

object Source {

  def getEvents[F[_]: ConcurrentEffect: ContextShift](app: String, stream: String) = {
    val config = KinesisConsumerSettings(stream, app, region = Region.EU_CENTRAL_1).right.get
    readFromKinesisStream[F](config)
      .map(processRecord)
      .map(event => event.event_id)
  }

  def processRecord(record: CommittableRecord): Event = {
    val string = try {
      StandardCharsets.UTF_8.decode(record.record.data()).toString.asRight
    } catch {
      case e: IllegalArgumentException =>
        e.getMessage.asLeft
    }

    string.toValidated.andThen(s => Event.parse(s)).getOrElse(throw new RuntimeException("bad event"))

  }
}
