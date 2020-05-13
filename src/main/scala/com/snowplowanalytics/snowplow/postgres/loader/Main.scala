package com.snowplowanalytics.snowplow.postgres.loader

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._

import com.snowplowanalytics.iglu.client.Client
import fs2.aws.kinesis.KinesisConsumerSettings

import com.snowplowanalytics.snowplow.postgres.loader.streaming.{sink, source}

import software.amazon.awssdk.regions.Region


object Main extends IOApp {
  def run(args: List[String]): IO[ExitCode] = {
    Config.command.parse(args) match {
      case Right(Config(app, stream, jdbc, username, password)) =>
        val igluClient = Client.IgluCentral
        val xa = sink.getTransactor[IO](jdbc, username, password)
        KinesisConsumerSettings.apply(stream, app, Region.EU_CENTRAL_1) match {
          case Right(config) =>
            for {
              state <- streaming.PgState.init[IO]
              _ <- source.getEvents[IO](config)
                .observeEither(sink.badSink[IO], sink.eventsSink[IO](xa, state, igluClient))
                .compile
                .drain
            } yield ExitCode.Success
          case Left(error) =>
            IO.delay(System.err.println(error)).as(ExitCode.Error)
        }
      case Left(help) =>
        IO.delay(System.err.println(help.toString)).as(ExitCode.Error)
    }
  }
}
