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

import cats.implicits._

import io.circe.Json
import io.circe.parser.{parse => parseCirce}

import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.implicits._
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{BadRow, Payload}

import com.snowplowanalytics.snowplow.postgres.streaming.data.{BadData, Data}
import com.snowplowanalytics.snowplow.postgres.config.Cli
import com.snowplowanalytics.snowplow.postgres.config.LoaderConfig.Purpose

object ParserUtils {

  /**
    * Parse given item into a valid Loader's record, either enriched event or self-describing JSON,
    * depending on purpose of the Loader
    */
  def parseItem(kind: Purpose, item: String): Either[BadData, Data] =
    kind match {
      case Purpose.Enriched =>
        parseEventString(item).map(Data.Snowplow.apply)
      case Purpose.SelfDescribing =>
        parseJson(item).map(Data.SelfDescribing.apply)
    }

  private def parseEventString(s: String): Either[BadData, Event] =
    Event.parse(s).toEither.leftMap { error =>
      val badRow = BadRow.LoaderParsingError(Cli.processor, error, Payload.RawPayload(s))
      BadData.BadEnriched(badRow)
    }

  private def parseJson(s: String): Either[BadData, SelfDescribingData[Json]] =
    parseCirce(s)
      .leftMap(_.show)
      .flatMap(json => SelfDescribingData.parse[Json](json).leftMap(_.message(json.noSpaces)))
      .leftMap(error => BadData.BadJson(s, error))
}
