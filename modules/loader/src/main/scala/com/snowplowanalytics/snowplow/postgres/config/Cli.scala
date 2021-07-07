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

import java.nio.file.{InvalidPathException, Paths}
import java.util.Base64

import cats.data.{EitherT, ValidatedNel}
import cats.implicits._

import cats.effect.{Clock, Sync}

import com.typesafe.config.{Config => LightbendConfig, ConfigFactory}

import io.circe.Json
import io.circe.parser.{parse => jsonParse}
import io.circe.config.parser.{decode => hoconDecode}

import scala.io.Source

import com.snowplowanalytics.iglu.client.Client

import com.snowplowanalytics.snowplow.badrows.Processor

import com.monovore.decline._

import com.snowplowanalytics.snowplow.postgres.generated.BuildInfo

case class Cli[F[_]](config: LoaderConfig, iglu: Client[F, Json])

object Cli {

  val processor = Processor(BuildInfo.name, BuildInfo.version)

  /** Parse list of arguments, validate against schema and initialize */
  def parse[F[_]: Sync: Clock](args: List[String]): EitherT[F, String, Cli[F]] =
    command.parse(args) match {
      case Left(help)       => EitherT.leftT[F, Cli[F]](help.show)
      case Right(rawConfig) => fromRawConfig(rawConfig)
    }

  private def fromRawConfig[F[_]: Sync: Clock](rawConfig: RawConfig): EitherT[F, String, Cli[F]] =
    for {
      resolverJson <- loadJson(rawConfig.resolver).toEitherT[F]
      igluClient <- Client.parseDefault[F](resolverJson).leftMap(_.show)
      configHocon <- loadHocon(rawConfig.config).toEitherT[F]
      appConfig <- hoconDecode[LoaderConfig](configHocon).leftMap(e => s"Could not parse config: ${e.show}").toEitherT[F]
    } yield Cli(appConfig, igluClient)

  private def parseSource(string: String, encoded: Boolean): ValidatedNel[String, Source] = {
    val result =
      if (encoded)
        Either
          .catchOnly[IllegalArgumentException](new String(Base64.getDecoder.decode(string)))
          .leftMap(_.getMessage)
          .map(Source.fromString)
      else Either.catchOnly[InvalidPathException](Paths.get(string)).leftMap(_.getMessage).map(p => Source.fromFile(p.toFile))
    result.leftMap(error => s"Cannot parse as ${if (encoded) "base64-encoded JSON" else "FS path"}: $error").toValidatedNel
  }

  private def loadJson(source: Source): Either[String, Json] =
    for {
      text <- Either
        .catchNonFatal(source.mkString)
        .leftMap(e => s"Could not read config: ${e.getMessage}")
      parsed <- jsonParse(text).leftMap(_.show)
    } yield parsed

  private def loadHocon(source: Source): Either[String, LightbendConfig] =
    for {
      text <- Either
        .catchNonFatal(source.mkString)
        .leftMap(e => s"Could not read config: ${e.getMessage}")
      resolved <- Either
        .catchNonFatal(ConfigFactory.parseString(text).resolve)
        .leftMap(e => s"Could not parse config: ${e.getMessage}")
    } yield namespaced(ConfigFactory.load(namespaced(resolved.withFallback(namespaced(ConfigFactory.load())))))

  /** Optionally give precedence to configs wrapped in a "snowplow" block. To help avoid polluting config namespace */
  private def namespaced(config: LightbendConfig): LightbendConfig =
    config.getConfig("snowplow").withFallback(config)

  val resolver = Opts.option[String](
    long = "resolver",
    help = "Iglu Resolver JSON config, FS path or base64-encoded"
  )

  val config = Opts.option[String](
    long = "config",
    help = "Self-describing JSON configuration"
  )

  val base64 = Opts
    .flag(
      long = "base64",
      help = "Configuration passed as Base64-encoded string, not as file path"
    )
    .orFalse

  /** Temporary, pure config */
  private case class RawConfig(config: Source, resolver: Source)

  private val command: Command[RawConfig] =
    Command[(String, String, Boolean)](BuildInfo.name, BuildInfo.version)((config, resolver, base64).tupled).mapValidated {
      case (cfg, res, enc) =>
        (parseSource(cfg, enc), parseSource(res, enc)).mapN(RawConfig.apply)
    }
}
