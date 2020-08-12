/*
 * Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.postgres.config

import java.nio.file.{InvalidPathException, Files, Path, Paths}
import java.util.Base64

import cats.data.{EitherT, ValidatedNel}
import cats.implicits._

import cats.effect.{Sync, Clock}

import io.circe.Json
import io.circe.syntax._
import io.circe.parser.{parse => jsonParse}

import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.implicits._

import com.snowplowanalytics.iglu.client.Client

import com.snowplowanalytics.snowplow.badrows.Processor

import com.monovore.decline._

import com.snowplowanalytics.snowplow.postgres.generated.BuildInfo

case class Cli[F[_]](config: LoaderConfig, iglu: Client[F, Json], debug: Boolean)

object Cli {

  val processor = Processor(BuildInfo.name, BuildInfo.version)

  /** Parse list of arguments, validate against schema and initialize */
  def parse[F[_]: Sync: Clock](args: List[String]): EitherT[F, String, Cli[F]] =
    command.parse(args) match {
      case Left(help) => EitherT.leftT[F, Cli[F]](help.show)
      case Right(rawConfig) => fromRawConfig(rawConfig)
    }

  private def fromRawConfig[F[_]: Sync: Clock](rawConfig: RawConfig): EitherT[F, String, Cli[F]] = {
    for {
      resolverJson <- PathOrJson.load(rawConfig.resolver)
      igluClient <- Client.parseDefault[F](resolverJson).leftMap(_.show)
      configJson <- PathOrJson.load(rawConfig.config)
      configData <- SelfDescribingData.parse(configJson).leftMap(e => s"Configuration JSON is not self-describing, ${e.message(configJson.noSpaces)}").toEitherT[F]
      _ <- igluClient.check(configData).leftMap(e => s"Iglu validation failed with following error\n: ${e.asJson.spaces2}")
      appConfig <- configData.data.as[LoaderConfig].toEitherT[F].leftMap(e => s"Error while decoding configuration JSON, ${e.show}")
    } yield Cli(appConfig, igluClient, rawConfig.debug)
  }

  /** Config files for Loader can be passed either as FS path
   * or as base64-encoded JSON (if `--base64` is provided) */
  type PathOrJson = Either[Path, Json]

  object PathOrJson {
    def parse(string: String, encoded: Boolean): ValidatedNel[String, PathOrJson] = {
      val result = if (encoded)
        Either
          .catchOnly[IllegalArgumentException](new String(Base64.getDecoder.decode(string)))
          .leftMap(_.getMessage)
          .flatMap(s => jsonParse(s).leftMap(_.show))
          .map(_.asRight)
      else Either.catchOnly[InvalidPathException](Paths.get(string).asLeft).leftMap(_.getMessage)
      result
        .leftMap(error => s"Cannot parse as ${if (encoded) "base64-encoded JSON" else "FS path"}: $error")
        .toValidatedNel
    }

    def load[F[_]: Sync](value: PathOrJson): EitherT[F, String, Json] =
      value match {
        case Right(json) =>
          EitherT.rightT[F, String](json)
        case Left(path) =>
          Either
            .catchNonFatal(new String(Files.readAllBytes(path)))
            .leftMap(e => s"Cannot read the file path: $e")
            .flatMap(s => jsonParse(s).leftMap(_.show))
            .toEitherT[F]
      }
  }

  val resolver = Opts.option[String](
    long = "resolver",
    help = "Iglu Resolver JSON config, FS path or base64-encoded"
  )

  val config = Opts.option[String](
    long = "config",
    help = "Self-describing JSON configuration"
  )

  val base64 = Opts.flag(
    long = "base64",
    help = "Configuration passed as Base64-encoded string, not as file path"
  ).orFalse

  val debug = Opts.flag(
    long = "debug",
    help = "Show verbose SQL logging"
  ).orFalse

  /** Temporary, pure config */
  private case class RawConfig(config: PathOrJson, resolver: PathOrJson, debug: Boolean)

  private val command: Command[RawConfig] =
    Command[(String, String, Boolean, Boolean)](BuildInfo.name, BuildInfo.version)((config, resolver, base64, debug).tupled)
      .mapValidated { case (cfg, res, enc, deb) =>
        (PathOrJson.parse(cfg, enc), PathOrJson.parse(res, enc), deb.validNel).mapN(RawConfig.apply)
      }
}
