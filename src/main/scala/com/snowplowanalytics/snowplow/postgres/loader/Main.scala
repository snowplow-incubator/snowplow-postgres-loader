package com.snowplowanalytics.snowplow.postgres.loader

import cats.data.EitherT
import cats.effect.{Clock, ConcurrentEffect, ContextShift, ExitCode, IO, Sync}
import cats.effect.implicits._
import cats.implicits._
import com.monovore.decline.effect.CommandIOApp
import com.monovore.decline.Opts
import com.snowplowanalytics.iglu.client.{CirceValidator, Client, Resolver}
import com.snowplowanalytics.iglu.client.resolver.registries.Registry.EmbeddedRegistry
import com.snowplowanalytics.iglu.core.circe.implicits._
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.postgres.loader.Options._
import com.snowplowanalytics.snowplow.postgres.loader.generated.ProjectSettings
import com.snowplowanalytics.snowplow.postgres.loader.streaming.{PgState, sink, source}
import doobie.util.transactor.Transactor
import fs2.aws.kinesis.KinesisConsumerSettings
import io.circe.syntax._
import io.circe.parser._
import io.circe.generic.auto._
import io.circe.Json
import org.dhallj.circe._
import org.dhallj.syntax._
import software.amazon.awssdk.regions.Region

object Main
    extends CommandIOApp(
      ProjectSettings.name,
      ProjectSettings.version
    ) {

  override def main: Opts[IO[ExitCode]] =
    (config, resolver, dhall).mapN {
      case (dhallCP, _, true)        => parseDhallConfig[IO](dhallCP).flatMap(r => program[IO](r._1, r._2))
      case (sdjCP, Some(rCP), false) => parseSelfDescJsonConfig[IO](sdjCP, rCP).flatMap(r => program[IO](r._1, r._2))
      case _                         => exitWithError[IO]("Invalid input parameters combination")
    }

  def program[F[_]: ConcurrentEffect: ContextShift: Clock](conf: Config, igluClient: Client[F, Json]): F[ExitCode] =
    for {
      xa <- Sync[F].delay(sink.getTransactor[F](conf.host, conf.port, conf.database, conf.username, conf.password))
      kcOrError = KinesisConsumerSettings(conf.stream, conf.appName, Region.EU_CENTRAL_1)
      ec <- kcOrError.fold(t => exitWithError(t.getMessage), kc => stream(kc, xa, igluClient))
    } yield ec

  def stream[F[_]: ConcurrentEffect: ContextShift: Clock](
    kinesisConsumerSettings: KinesisConsumerSettings,
    xa: Transactor[F],
    igluClient: Client[F, Json]
  ): F[ExitCode] =
    for {
      state <- PgState.init[F]
      _ <- source
        .getEvents[F](kinesisConsumerSettings)
        .observeEither(sink.badSink[F], sink.eventsSink[F](xa, state, igluClient))
        .compile
        .drain
    } yield ExitCode.Success

  def parseDhallConfig[F[_]: Sync](configPath: String): F[(Config, Client[F, Json])] = {
    val mkResolveConfig: Json => SelfDescribingData[Json] =
      SelfDescribingData[Json](
        SchemaKey("com.snowplowanalytics.iglu", "resolver-config", "jsonschema", SchemaVer.Full(1, 0, 0)),
        _
      )

    (for {
      confString   <- EitherT.liftF(readFileToString(configPath))
      dhallConf    <- EitherT.fromEither[F](confString.parseExpr.flatMap(_.resolve).map(_.normalize))
      jsonConf     <- EitherT.fromOption[F](Converter(dhallConf), new Throwable("Invalid Dhall config"))
      conf         <- EitherT.fromEither[F](jsonConf.as[Config]).leftWiden[Throwable]
      resolverConf <- EitherT.fromEither[F](jsonConf.hcursor.get[Json]("igluResolver")).leftWiden[Throwable]
      resolver     <- EitherT(Resolver.parse[F](mkResolveConfig(resolverConf).asJson)).leftWiden[Throwable]
      igluClient = Client[F, Json](resolver.copy(repos = EmbeddedRegistry :: resolver.repos), CirceValidator)
    } yield (conf, igluClient)).value.rethrow
  }

  def parseSelfDescJsonConfig[F[_]: Sync: Clock](
    configPath: String,
    resolverConfigPath: String
  ): F[(Config, Client[F, Json])] =
    (for {
      resolverConfString <- EitherT.liftF(readFileToString(resolverConfigPath))
      resolverConfJson   <- EitherT.fromEither[F](parse(resolverConfString)).leftWiden[Throwable]
      resolver           <- EitherT(Resolver.parse[F](resolverConfJson))
      igluClient = Client[F, Json](resolver.copy(repos = EmbeddedRegistry :: resolver.repos), CirceValidator)
      confString <- EitherT.liftF(readFileToString(configPath))
      confJson   <- EitherT.fromEither[F](parse(confString)).leftWiden[Throwable]
      confSdd    <- EitherT.fromEither[F](SelfDescribingData.parse(confJson).leftMap(e => new Throwable(e.code)))
      _          <- igluClient.check(confSdd).leftMap(e => new Throwable(e.getMessage))
      conf       <- EitherT.fromEither[F](confSdd.data.as[Config]).leftWiden[Throwable]
    } yield (conf, igluClient)).value.rethrow

  def readFileToString[F[_]: Sync](path: String): F[String] =
    Sync[F]
      .delay(scala.io.Source.fromFile(path))
      .bracket(f => Sync[F].delay(f.getLines.mkString))(f => Sync[F].delay(f.close).handleError(_ => ()))

  def exitWithError[F[_]: Sync](error: String): F[ExitCode] =
    Sync[F].delay(System.err.println(error)).as(ExitCode.Error)

}
