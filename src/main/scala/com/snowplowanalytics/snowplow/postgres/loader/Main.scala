package com.snowplowanalytics.snowplow.postgres.loader

import cats.effect.{ExitCode, IO, IOApp, Resource, Fiber }
import cats.implicits._

import fs2.aws.kinesis.KinesisConsumerSettings

import software.amazon.awssdk.regions.Region


object Main extends IOApp {
  def run(args: List[String]): IO[ExitCode] = {
    Options.command.parse(args) match {
      case Right(Options.Config(app, stream, jdbc, username, password)) =>
        val xa = Sink.getTransaction[IO](jdbc, username, password)
        KinesisConsumerSettings.apply(stream, app, Region.EU_CENTRAL_1) match {
          case Right(config) =>
            Source.getEvents[IO](config)
              .observeEither(Source.badSink[IO], Source.eventsSink[IO](xa))
              .compile
              .drain
              .as(ExitCode.Success)
          case Left(error) =>
            IO.delay(System.err.println(error)).as(ExitCode.Error)
        }
      case Left(help) =>
        IO.delay(System.err.println(help.toString)).as(ExitCode.Error)
    }
  }
}
