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
package com.snowplowanalytics.snowplow.postgres.utils

import cats.Monad
import cats.implicits._
import cats.data.{EitherT, NonEmptyList}
import cats.effect.Clock

import io.circe.Json
import io.circe.parser.{parse => parseCirce}

import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.implicits._
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{BadRow, Payload, Failure}

import com.snowplowanalytics.snowplow.postgres.streaming.data.Data
import com.snowplowanalytics.snowplow.postgres.streaming.TimeUtils
import com.snowplowanalytics.snowplow.postgres.config.Cli
import com.snowplowanalytics.snowplow.postgres.config.LoaderConfig.Purpose

object ParserUtils {

  /**
    * Parse given item into a valid Loader's record, either enriched event or self-describing JSON,
    * depending on purpose of the Loader
    */
  def parseItem[F[_]: Clock: Monad](kind: Purpose, item: String): F[Either[BadRow, Data]] =
    kind match {
      case Purpose.Enriched =>
        Monad[F].pure(parseEventString(item).map[Data](Data.Snowplow.apply))
      case Purpose.SelfDescribing =>
        parseJson(item).map[Data](Data.SelfDescribing.apply).value
    }

  private def parseEventString(s: String): Either[BadRow, Event] =
    Event.parse(s).toEither.leftMap { error =>
      BadRow.LoaderParsingError(Cli.processor, error, Payload.RawPayload(s))
    }

  private def parseJson[F[_]: Clock: Monad](s: String): EitherT[F, BadRow, SelfDescribingData[Json]] =
    EitherT.fromEither[F](parseCirce(s))
      .leftMap(_.show)
      .subflatMap(json => SelfDescribingData.parse[Json](json).leftMap(_.message(json.noSpaces)))
      .leftSemiflatMap { error =>
        TimeUtils.now.map { now =>
          val failure = Failure.GenericFailure(
            now,
            NonEmptyList.of(error)
          )
          BadRow.GenericError(Cli.processor, failure, Payload.RawPayload(s))
        }
      }
}
