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
package com.snowplowanalytics.snowplow.postgres.streaming

import com.snowplowanalytics.iglu.core.SelfDescribingData

import io.circe.Json

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.BadRow

object data {

  /** Kind of data flowing through the Loader */
  sealed trait Data extends Product with Serializable {
    def snowplow: Boolean =
      this match {
        case _: Data.Snowplow       => true
        case _: Data.SelfDescribing => false
      }
  }

  object Data {
    case class Snowplow(data: Event) extends Data
    case class SelfDescribing(data: SelfDescribingData[Json]) extends Data
  }

  /** Data that for some reasons cannot be inserted into DB */
  sealed trait BadData extends Throwable with Product with Serializable
  object BadData {

    /** Typical Snowplow bad row (Loader Iglu Error etc) */
    case class BadEnriched(data: BadRow) extends BadData

    /** Non-enriched error */
    case class BadJson(payload: String, error: String) extends BadData
  }
}
