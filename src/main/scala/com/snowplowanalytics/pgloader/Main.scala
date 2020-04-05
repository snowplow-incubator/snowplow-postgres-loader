package com.snowplowanalytics.pgloader

// Scala third-party
import java.util.UUID

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._

import doobie.implicits._


object Main extends IOApp {
  def run(args: List[String]): IO[ExitCode] = {
    Options.command.parse(args) match {
      case Right(Options.Config(app, stream, jdbc, password)) =>
        val xa = Sink.getTransaction[IO](jdbc, password)
        Source.getEvents[IO](app, stream)
          .evalMap(id => Sink.insert(id).transact(xa))
          .compile
          .drain
          .as(ExitCode.Success)
      case Left(help) =>
        IO.delay(System.err.println(help.toString)).as(ExitCode.Error)
    }
  }
}
